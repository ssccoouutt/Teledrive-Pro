import re
import os
import io
import random
import asyncio
import traceback
import time
from telegram import Update, MessageEntity
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import Flow

# Configuration (unchanged from original)
BOT_TOKEN = "7846379611:AAGzu4KM-Aq699Q8aHNt29t0YbTnDKbkXbI"
TOKEN_PATH = 'token.json'
CREDENTIALS_PATH = 'credentials.json'
PHASE2_SOURCE = '1TaBiq6z01lLP-znWMz1S_RwJ1PkLRyjk'
PHASE3_SOURCE = '12V7EnRIYcSgEtt0PR5fhV8cO22nzYuiv'
SHORT_LINKS = ["rb.gy/cd8ugy", "bit.ly/3UcvhlA", "t.ly/CfcVB", "cutt.ly/Kee3oiLO"]
TARGET_CHANNEL = "@techworld196"
BANNED_FILE_ID = '1B5GAAtzpuH_XNGyUiJIMDlB9hJfxkg8r'
SCOPES = ['https://www.googleapis.com/auth/drive']

# Constants (unchanged from original)
MAX_RETRIES = 3
RETRY_DELAY = 10  # seconds
CHUNK_SIZE = 20  # Number of files to process at once

# Authorization state
AUTH_STATE = 1
pending_authorizations = {}

# ========== NEW AUTHORIZATION FUNCTIONS ==========
def get_drive_service():
    """Initialize and return Google Drive service with OAuth flow"""
    creds = None
    if os.path.exists(TOKEN_PATH):
        creds = Credentials.from_authorized_user_file(TOKEN_PATH, SCOPES)
    
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(TOKEN_PATH, 'w') as token:
                token.write(creds.to_json())
        else:
            raise Exception('Google Drive authorization required. Use /auth to authenticate.')
    
    return build('drive', 'v3', credentials=creds)

async def auth_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start Google Drive authorization process"""
    flow = Flow.from_client_secrets_file(
        CREDENTIALS_PATH,
        scopes=SCOPES,
        redirect_uri='urn:ietf:wg:oauth:2.0:oob'
    )
    auth_url, _ = flow.authorization_url(prompt='consent')
    pending_authorizations[update.effective_user.id] = flow
    
    await update.message.reply_text(
        "🔑 *Google Drive Authorization Required*\n\n"
        "1. Click this link to authorize:\n"
        f"{auth_url}\n\n"
        "2. After approving, copy the authorization code\n"
        "3. Send the code back to this chat\n\n"
        "⚠️ Note: You may see an 'unverified app' warning. Click 'Advanced' then 'Continue'.",
        parse_mode='Markdown',
        disable_web_page_preview=True
    )
    return AUTH_STATE

async def handle_auth_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle received authorization code"""
    user_id = update.effective_user.id
    code = update.message.text.strip()
    
    if user_id not in pending_authorizations:
        await update.message.reply_text("❌ No active authorization session. Start with /auth")
        return ConversationHandler.END
    
    try:
        flow = pending_authorizations[user_id]
        flow.fetch_token(code=code)
        creds = flow.credentials
        
        with open(TOKEN_PATH, 'w') as token_file:
            token_file.write(creds.to_json())
        
        del pending_authorizations[user_id]
        await update.message.reply_text("✅ Authorization successful! Bot is now ready to use.")
        
    except Exception as e:
        await update.message.reply_text(f"❌ Authorization failed: {str(e)}")
    
    return ConversationHandler.END

async def cancel_auth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel authorization process"""
    user_id = update.effective_user.id
    if user_id in pending_authorizations:
        del pending_authorizations[user_id]
    
    await update.message.reply_text("❌ Authorization process cancelled.")
    return ConversationHandler.END

# ========== ORIGINAL FUNCTIONS (COMPLETELY UNCHANGED) ==========
def initialize_banned_items(service):
    """Load banned items list from Google Drive"""
    try:
        request = service.files().get_media(fileId=BANNED_FILE_ID)
        banned_file = request.execute()
        return banned_file.decode('utf-8').splitlines()
    except Exception as e:
        print(f"Error loading banned items: {str(e)}")
        return []

def save_banned_items(service, banned_items):
    """Save banned items list to Google Drive"""
    try:
        content = '\n'.join(banned_items).encode('utf-8')
        media = MediaIoBaseUpload(io.BytesIO(content), mimetype='text/plain')
        service.files().update(fileId=BANNED_FILE_ID, media_body=media).execute()
    except Exception as e:
        print(f"Error saving banned items: {str(e)}")

def extract_folder_id(url):
    """Extract folder ID from Google Drive URL with multiple pattern support"""
    patterns = [
        r'/folders/([a-zA-Z0-9-_]+)',        # Standard /folders/ pattern
        r'[?&]id=([a-zA-Z0-9-_]+)',          # ?id= or &id= pattern
        r'/folderview[?&]id=([a-zA-Z0-9-_]+)' # /folderview?id= pattern
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return None

def extract_file_id(url):
    """Extract file ID from Google Drive URL"""
    match = re.search(r'/file/d/([a-zA-Z0-9-_]+)', url)
    return match.group(1) if match else None

def should_skip_item(name, banned_items):
    """Check if item should be skipped based on banned list"""
    return name in banned_items

def execute_with_retry(func, *args, **kwargs):
    """Execute a function with retry mechanism"""
    last_exception = None
    for attempt in range(MAX_RETRIES):
        try:
            return func(*args, **kwargs).execute()
        except HttpError as e:
            if e.resp.status in [500, 502, 503, 504] or 'timed out' in str(e).lower():
                last_exception = e
                print(f"Attempt {attempt + 1} failed, retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
                continue
            raise
        except Exception as e:
            if 'timed out' in str(e).lower() and attempt < MAX_RETRIES - 1:
                last_exception = e
                print(f"Attempt {attempt + 1} failed, retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
                continue
            raise
    
    raise Exception(f"Operation failed after {MAX_RETRIES} attempts. Last error: {str(last_exception)}")

def copy_folder(service, folder_id, banned_items):
    """Copy a folder and its contents with retry mechanism"""
    try:
        folder = execute_with_retry(service.files().get, fileId=folder_id, fields='name')
        new_folder = service.files().create(body={
            'name': folder['name'],
            'mimeType': 'application/vnd.google-apps.folder'
        }).execute()
        new_folder_id = new_folder['id']

        copy_folder_contents(service, folder_id, new_folder_id, banned_items)
        subfolders = get_all_subfolders_recursive(service, new_folder_id)
        
        for subfolder_id in subfolders:
            copy_files_only(service, PHASE2_SOURCE, subfolder_id, banned_items, overwrite=True)

        copy_bonus_content(service, PHASE3_SOURCE, new_folder_id, banned_items, overwrite=True)
        rename_files_and_folders(service, new_folder_id)
        
        for subfolder_id in subfolders:
            rename_files_and_folders(service, subfolder_id)

        return new_folder_id
    except Exception as e:
        raise Exception(f"Copy failed: {str(e)}")

def get_all_subfolders_recursive(service, folder_id):
    """Get all subfolder IDs recursively with chunked processing"""
    subfolders = []
    queue = [folder_id]
    
    while queue:
        current_folder = queue.pop(0)
        page_token = None
        
        while True:
            try:
                response = execute_with_retry(service.files().list,
                    q=f"'{current_folder}' in parents and mimeType='application/vnd.google-apps.folder'",
                    fields='nextPageToken, files(id)',
                    pageSize=CHUNK_SIZE
                )
                
                for folder in response.get('files', []):
                    subfolders.append(folder['id'])
                    queue.append(folder['id'])
                
                page_token = response.get('nextPageToken')
                if not page_token:
                    break
            except Exception as e:
                print(f"Error getting subfolders: {str(e)}")
                break
    
    return subfolders

def copy_files_only(service, source_id, dest_id, banned_items, overwrite=False):
    """Copy files from source to destination with chunked processing"""
    page_token = None
    while True:
        try:
            response = execute_with_retry(service.files().list,
                q=f"'{source_id}' in parents",
                fields='nextPageToken, files(id, name, mimeType)',
                pageSize=CHUNK_SIZE,
                pageToken=page_token
            )
            
            for item in response.get('files', []):
                if should_skip_item(item['name'], banned_items):
                    continue
                if item['mimeType'] != 'application/vnd.google-apps.folder':
                    copy_item_to_folder(service, item, dest_id, banned_items, overwrite)
            
            page_token = response.get('nextPageToken')
            if not page_token:
                break
        except Exception as e:
            print(f"Error copying files: {str(e)}")
            break

def copy_bonus_content(service, source_id, dest_id, banned_items, overwrite=False):
    """Copy bonus content to destination with chunked processing"""
    page_token = None
    while True:
        try:
            response = execute_with_retry(service.files().list,
                q=f"'{source_id}' in parents",
                fields='nextPageToken, files(id, name, mimeType)',
                pageSize=CHUNK_SIZE,
                pageToken=page_token
            )
            
            for item in response.get('files', []):
                if should_skip_item(item['name'], banned_items):
                    continue
                copy_item_to_folder(service, item, dest_id, banned_items, overwrite)
            
            page_token = response.get('nextPageToken')
            if not page_token:
                break
        except Exception as e:
            print(f"Error copying bonus content: {str(e)}")
            break

def copy_item_to_folder(service, item, dest_folder_id, banned_items, overwrite=False):
    """Copy individual item to destination folder with retry"""
    try:
        if overwrite:
            existing = execute_with_retry(service.files().list,
                q=f"name='{item['name']}' and '{dest_folder_id}' in parents",
                fields='files(id)'
            ).get('files', [])
            
            for file in existing:
                execute_with_retry(service.files().delete, fileId=file['id'])

        if item['mimeType'] == 'application/vnd.google-apps.folder':
            new_folder = service.files().create(body={
                'name': item['name'],
                'parents': [dest_folder_id],
                'mimeType': 'application/vnd.google-apps.folder'
            }).execute()
            copy_bonus_content(service, item['id'], new_folder['id'], banned_items, overwrite)
        else:
            service.files().copy(
                fileId=item['id'],
                body={'parents': [dest_folder_id]}
            ).execute()
    except Exception as e:
        print(f"Error copying {item['name']}: {str(e)}")

def copy_folder_contents(service, source_id, dest_id, banned_items):
    """Copy all contents from source to destination folder with chunked processing"""
    page_token = None
    while True:
        try:
            response = execute_with_retry(service.files().list,
                q=f"'{source_id}' in parents",
                fields='nextPageToken, files(id, name, mimeType)',
                pageSize=CHUNK_SIZE,
                pageToken=page_token
            )
            
            for item in response.get('files', []):
                if should_skip_item(item['name'], banned_items):
                    continue
                    
                if item['mimeType'] == 'application/vnd.google-apps.folder':
                    new_subfolder = service.files().create(body={
                        'name': item['name'],
                        'parents': [dest_id],
                        'mimeType': 'application/vnd.google-apps.folder'
                    }).execute()
                    copy_folder_contents(service, item['id'], new_subfolder['id'], banned_items)
                else:
                    service.files().copy(
                        fileId=item['id'],
                        body={'parents': [dest_id]}
                    ).execute()
            
            page_token = response.get('nextPageToken')
            if not page_token:
                break
        except Exception as e:
            print(f"Error copying folder contents: {str(e)}")
            break

def rename_files_and_folders(service, folder_id):
    """Rename files and folders with both @mentions and .mp4 patterns with chunked processing"""
    page_token = None
    while True:
        try:
            response = execute_with_retry(service.files().list,
                q=f"'{folder_id}' in parents",
                fields='nextPageToken, files(id, name, mimeType)',
                pageSize=CHUNK_SIZE,
                pageToken=page_token
            )
            
            for item in response.get('files', []):
                try:
                    current_name = item['name']
                    new_name = current_name
                    
                    # First check for @[any text] pattern
                    at_pattern = re.compile(r'@\w+')
                    at_match = at_pattern.search(current_name)
                    
                    if at_match:
                        # Replace any @mention with @TechZoneX
                        new_name = at_pattern.sub('@TechZoneX', current_name)
                    elif item['mimeType'] == 'video/mp4' and current_name.endswith('.mp4'):
                        # Only add watermark if not an @mention file and is mp4
                        new_name = current_name.replace('.mp4', ' (Telegram@TechZoneX).mp4')
                    
                    if new_name != current_name:
                        service.files().update(
                            fileId=item['id'],
                            body={'name': new_name}
                        ).execute()
                except Exception as e:
                    print(f"Error renaming {item['name']}: {str(e)}")
                    continue
            
            page_token = response.get('nextPageToken')
            if not page_token:
                break
        except Exception as e:
            print(f"Error listing files for renaming: {str(e)}")
            break

def validate_entity_positions(text, entities):
    """Ensure entities align with UTF-16 character boundaries"""
    if not entities:
        return []
    
    valid_entities = []
    text_utf16 = text.encode('utf-16-le')
    
    for entity in entities:
        try:
            start = entity.offset * 2
            end = start + (entity.length * 2)
            
            if start >= len(text_utf16) or end > len(text_utf16):
                continue
                
            # Validate the substring
            _ = text_utf16[start:end].decode('utf-16-le')
            
            if entity.type == MessageEntity.TEXT_LINK:
                valid_entities.append(MessageEntity(
                    type=entity.type,
                    offset=entity.offset,
                    length=entity.length,
                    url=entity.url
                ))
            else:
                valid_entities.append(MessageEntity(
                    type=entity.type,
                    offset=entity.offset,
                    length=entity.length
                ))
        except Exception as e:
            print(f"Skipping invalid entity: {str(e)}")
            continue
            
    return valid_entities

def filter_entities(entities):
    """Filter to only basic formatting entities"""
    allowed_types = {
        MessageEntity.BOLD,
        MessageEntity.ITALIC,
        MessageEntity.CODE,
        MessageEntity.PRE,
        MessageEntity.UNDERLINE,
        MessageEntity.STRIKETHROUGH,
        MessageEntity.TEXT_LINK
    }
    return [e for e in entities if e.type in allowed_types] if entities else []

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle incoming messages"""
    message = update.message
    if not message or (message.text and message.text.startswith('/')):
        return

    original_text = message.caption or message.text or ''
    original_entities = message.caption_entities if message.caption else message.entities
    drive_links = []

    try:
        drive_service = get_drive_service()
        banned_items = initialize_banned_items(drive_service)

        if original_text:
            # Process Google Drive links with multiple pattern support
            url_matches = list(re.finditer(
                r'https?://(?:drive\.google\.com/(?:drive/folders/|folderview\?id=|.*[?&]id=)|.*\.google\.com/open\?id=)[\w-]+[^\s>]*',
                original_text
            ))
            
            for match in url_matches:
                url = match.group()
                folder_id = extract_folder_id(url)
                
                if folder_id:
                    try:
                        new_folder_id = await asyncio.get_event_loop().run_in_executor(
                            None, copy_folder, drive_service, folder_id, banned_items
                        )
                        random_link = random.choice(SHORT_LINKS)
                        new_url = f'https://drive.google.com/drive/folders/{new_folder_id} {random_link}'
                        drive_links.append((url, new_url))
                        original_text = original_text.replace(url, new_url)
                    except Exception as e:
                        await message.reply_text(f"⚠️ Error processing {url}: {str(e)}")
                        continue

            # Truncate after last drive link if any
            if drive_links:
                last_pos = original_text.rfind(drive_links[-1][1]) + len(drive_links[-1][1])
                final_text = original_text[:last_pos].strip()
            else:
                final_text = original_text

            # Process entities
            filtered_entities = filter_entities(original_entities)
            valid_entities = validate_entity_positions(final_text, filtered_entities)
        else:
            final_text = ''
            valid_entities = []

        # Prepare message arguments
        send_args = {
            'chat_id': TARGET_CHANNEL,
            'disable_notification': True,
            'caption': final_text,
            'caption_entities': valid_entities
        }

        # Forward message with appropriate media type
        if message.photo:
            await context.bot.send_photo(
                photo=message.photo[-1].file_id,
                **send_args
            )
        elif message.video:
            await context.bot.send_video(
                video=message.video.file_id,
                **send_args
            )
        elif message.document:
            await context.bot.send_document(
                document=message.document.file_id,
                **send_args
            )
        elif message.audio:
            await context.bot.send_audio(
                audio=message.audio.file_id,
                **send_args
            )
        else:
            await context.bot.send_message(
                text=final_text,
                entities=valid_entities,
                disable_notification=True,
                chat_id=TARGET_CHANNEL
            )

    except Exception as e:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=f"⚠️ Processing error: {str(e)[:200]}"
        )

async def ban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ban a file or folder from being processed"""
    try:
        if not context.args:
            await update.message.reply_text("❌ Usage: /ban <filename_or_folder_or_drive_link>")
            return

        input_text = ' '.join(context.args).strip()
        if not input_text:
            await update.message.reply_text("❌ Empty input provided")
            return

        drive_service = get_drive_service()
        banned_items = initialize_banned_items(drive_service)

        # Check if input is a Google Drive link
        file_id = extract_file_id(input_text)
        folder_id = extract_folder_id(input_text)
        
        item_name = input_text  # default to original input
        
        if file_id:
            try:
                file_info = execute_with_retry(drive_service.files().get, fileId=file_id, fields='name')
                item_name = file_info['name']
            except Exception as e:
                await update.message.reply_text(f"⚠️ Could not fetch file info: {str(e)}")
                return
        elif folder_id:
            try:
                folder_info = execute_with_retry(drive_service.files().get, fileId=folder_id, fields='name')
                item_name = folder_info['name']
            except Exception as e:
                await update.message.reply_text(f"⚠️ Could not fetch folder info: {str(e)}")
                return

        if item_name not in banned_items:
            banned_items.append(item_name)
            save_banned_items(drive_service, banned_items)
            response_text = f"✅ Banned: {item_name}"
        else:
            response_text = f"⚠️ Already banned: {item_name}"
        
        await update.message.reply_text(response_text)

    except Exception as e:
        await update.message.reply_text(f"⚠️ Ban failed: {str(e)}")

async def unban(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Unban a file or folder"""
    try:
        if not context.args:
            await update.message.reply_text("❌ Usage: /unban <filename_or_folder_or_drive_link>")
            return

        input_text = ' '.join(context.args).strip()
        if not input_text:
            await update.message.reply_text("❌ Empty input provided")
            return

        drive_service = get_drive_service()
        banned_items = initialize_banned_items(drive_service)

        # Check if input is a Google Drive link
        file_id = extract_file_id(input_text)
        folder_id = extract_folder_id(input_text)
        
        item_name = input_text  # default to original input
        
        if file_id:
            try:
                file_info = execute_with_retry(drive_service.files().get, fileId=file_id, fields='name')
                item_name = file_info['name']
            except Exception as e:
                await update.message.reply_text(f"⚠️ Could not fetch file info: {str(e)}")
                return
        elif folder_id:
            try:
                folder_info = execute_with_retry(drive_service.files().get, fileId=folder_id, fields='name')
                item_name = folder_info['name']
            except Exception as e:
                await update.message.reply_text(f"⚠️ Could not fetch folder info: {str(e)}")
                return

        if item_name in banned_items:
            banned_items.remove(item_name)
            save_banned_items(drive_service, banned_items)
            response_text = f"✅ Unbanned: {item_name}"
        else:
            response_text = f"⚠️ Not banned: {item_name}"
        
        await update.message.reply_text(response_text)

    except Exception as e:
        await update.message.reply_text(f"⚠️ Unban failed: {str(e)}")

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send welcome message with bot instructions"""
    await update.message.reply_text(
        "🚀 TechZoneX Auto Forward Bot\n\n"
        "Send any post with Google Drive links for processing!\n"
        "Admins:\n"
        "/auth - Authorize Google Drive\n"
        "/ban <name_or_link> - Block files/folders\n"
        "/unban <name_or_link> - Unblock files/folders"
    )

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle errors in the bot"""
    error = context.error
    tb_list = traceback.format_exception(type(error), error, error.__traceback__)
    tb_string = ''.join(tb_list)
    print(f"Exception occurred:\n{tb_string}")
    
    try:
        if update and update.effective_chat:
            await context.bot.send_message(
                chat_id=update.effective_chat.id,
                text="⚠️ An error occurred. Please check the format and try again."
            )
    except Exception as e:
        print(f"Error in error handler while sending message: {e}")

def main():
    """Start the bot"""
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Command handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("ban", ban))
    application.add_handler(CommandHandler("unban", unban))
    
    # Authorization conversation handler
    auth_conv = ConversationHandler(
        entry_points=[CommandHandler("auth", auth_command)],
        states={
            AUTH_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_auth_code)]
        },
        fallbacks=[CommandHandler("cancel", cancel_auth)]
    )
    application.add_handler(auth_conv)
    
    # Message handler
    application.add_handler(MessageHandler(
        filters.CAPTION | filters.TEXT | filters.PHOTO |
        filters.VIDEO | filters.Document.ALL | filters.AUDIO &
        ~filters.COMMAND,
        handle_message
    ))
    
    # Error handler
    application.add_error_handler(error_handler)
    
    # Start polling
    print("🤖 Bot is running with enhanced authentication flow...")
    application.run_polling()

if __name__ == "__main__":
    main()