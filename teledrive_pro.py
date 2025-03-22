import os
import logging
import re
import base64
from collections import defaultdict
from telegram import Update
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
    ConversationHandler
)
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
SCOPES = ['https://www.googleapis.com/auth/drive']
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
GOOGLE_CREDENTIALS = os.getenv('GOOGLE_CREDENTIALS')
CLIENT_SECRET_FILE = 'credentials.json'
TOKEN_DIR = 'tokens'

# Initialize logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Create necessary directories
os.makedirs(TOKEN_DIR, exist_ok=True)

# Global variables
pending_authorizations = {}
progress_data = defaultdict(dict)
user_sessions = {}

FILE_TYPES = {
    'application/pdf': 'PDF',
    'application/vnd.google-apps.document': 'Document',
    'application/vnd.google-apps.spreadsheet': 'Spreadsheet',
    'image/': 'Image',
    'video/': 'Video',
    'audio/': 'Audio',
    'text/': 'Text',
    'application/zip': 'Archive',
    'application/vnd.google-apps.folder': 'Folder'
}

# Create credentials.json if not exists
if GOOGLE_CREDENTIALS and not os.path.exists(CLIENT_SECRET_FILE):
    try:
        decoded = base64.b64decode(GOOGLE_CREDENTIALS).decode()
        with open(CLIENT_SECRET_FILE, 'w') as f:
            f.write(decoded)
    except Exception as e:
        logger.error(f"Failed to create credentials.json: {e}")
        raise

def get_user_token_path(user_id: int) -> str:
    return os.path.join(TOKEN_DIR, f'{user_id}.json')

def authorize_google_drive(user_id: int) -> Credentials:
    token_path = get_user_token_path(user_id)
    creds = None
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(token_path, 'w') as token:
                token.write(creds.to_json())
    return creds

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.message.from_user
    welcome_msg = (
        f"🚀 **Welcome {user.first_name}!**\n\n"
        "I can help you manage your Google Drive files directly from Telegram!\n\n"
        "🔑 First, you need to authorize your Google Account using /auth\n"
        "📁 Then send me a Google Drive folder link to start copying!"
    )
    await update.message.reply_text(welcome_msg, parse_mode='Markdown')

async def start_auth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    flow = Flow.from_client_secrets_file(
        CLIENT_SECRET_FILE,
        scopes=SCOPES,
        redirect_uri='urn:ietf:wg:oauth:2.0:oob'
    )
    auth_url, state = flow.authorization_url(
        prompt='consent',
        access_type='offline',
        include_granted_scopes='true'
    )
    pending_authorizations[user_id] = (flow, state)
    
    auth_instructions = (
        "🔑 **Authorization Required**\n\n"
        "1. Click this link to authorize:\n"
        f"{auth_url}\n\n"
        "2. After authorization, copy the code\n"
        "3. Paste it here like this: /code YOUR_CODE"
    )
    await update.message.reply_text(auth_instructions, parse_mode='Markdown')
    return 'WAITING_FOR_CODE'

async def handle_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    code = context.args[0] if context.args else None
    
    if not code:
        await update.message.reply_text("❌ Please provide an authorization code. Usage: /code YOUR_CODE")
        return ConversationHandler.END
    
    if user_id not in pending_authorizations:
        await update.message.reply_text("❌ No pending authorization. Start with /auth first.")
        return ConversationHandler.END
    
    flow, state = pending_authorizations[user_id]
    try:
        flow.fetch_token(code=code)
        creds = flow.credentials
        token_path = get_user_token_path(user_id)
        with open(token_path, 'w') as token_file:
            token_file.write(creds.to_json())
        del pending_authorizations[user_id]
        await update.message.reply_text("✅ Authorization successful! You can now use Drive features.")
    except Exception as e:
        await update.message.reply_text(f"❌ Authorization failed: {str(e)}")
    
    return ConversationHandler.END

async def cancel_auth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id in pending_authorizations:
        del pending_authorizations[user_id]
    await update.message.reply_text("❌ Authorization process cancelled.")
    return ConversationHandler.END

async def revoke_auth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    token_path = get_user_token_path(user_id)
    if os.path.exists(token_path):
        os.remove(token_path)
        await update.message.reply_text("✅ Your authorization has been revoked.")
    else:
        await update.message.reply_text("ℹ️ No active authorization found.")

async def handle_drive_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    chat_id = update.message.chat_id
    
    # Check authorization
    creds = authorize_google_drive(user_id)
    if not creds:
        await update.message.reply_text("❌ You need to authorize first using /auth")
        return
    
    try:
        # Extract folder ID
        link = update.message.text.strip()
        folder_id = re.search(r'/folders/([a-zA-Z0-9_-]+)', link).group(1)
        
        # Initialize Drive service
        service = build('drive', 'v3', credentials=creds)
        
        # Start processing
        progress_msg = await update.message.reply_text("⏳ Initializing folder copy...")
        
        # Store processing data
        progress_data[user_id] = {
            'total_files': 0,
            'processed_files': 0,
            'file_types': defaultdict(int),
            'total_size': 0,
            'message_id': progress_msg.message_id
        }
        
        # Start background copy process
        context.job_queue.run_once(
            lambda ctx: copy_folder_process(ctx, user_id, folder_id),
            when=0,
            data={'chat_id': chat_id, 'user_id': user_id}
        )
        
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)}")

async def copy_folder_process(context: ContextTypes.DEFAULT_TYPE, user_id: int, folder_id: str):
    job = context.job
    chat_id = job.data['chat_id']
    
    try:
        creds = authorize_google_drive(user_id)
        service = build('drive', 'v3', credentials=creds)
        
        # Count files and calculate size
        await update_progress(context, chat_id, user_id, "🔍 Analyzing folder contents...")
        total_files, total_size = count_files_and_size(service, folder_id)
        
        # Update progress data
        progress_data[user_id].update({
            'total_files': total_files,
            'total_size': total_size
        })
        
        # Start actual copying
        await update_progress(context, chat_id, user_id, "🚀 Starting folder copy...")
        await copy_folder(service, folder_id, None, user_id, context)
        
        # Final success message
        success_msg = (
            "✅ **Copy Complete!**\n\n"
            f"📂 Total Files: {total_files}\n"
            f"📦 Total Size: {format_size(total_size)}\n"
            f"📊 File Types:\n{format_file_types(progress_data[user_id]['file_types'])}"
        )
        await update_progress(context, chat_id, user_id, success_msg)
        
    except Exception as e:
        error_msg = f"❌ Copy failed: {str(e)}"
        await update_progress(context, chat_id, user_id, error_msg)
    finally:
        if user_id in progress_data:
            del progress_data[user_id]

def count_files_and_size(service, folder_id: str) -> tuple:
    total_files = 0
    total_size = 0
    page_token = None
    
    while True:
        response = service.files().list(
            q=f"'{folder_id}' in parents",
            fields="nextPageToken, files(id, mimeType, size)",
            pageToken=page_token
        ).execute()
        
        for file in response.get('files', []):
            if file['mimeType'] == 'application/vnd.google-apps.folder':
                sub_files, sub_size = count_files_and_size(service, file['id'])
                total_files += sub_files
                total_size += sub_size
            else:
                total_files += 1
                total_size += int(file.get('size', 0))
        
        page_token = response.get('nextPageToken')
        if not page_token:
            break
    
    return total_files, total_size

async def copy_folder(service, src_folder_id: str, dest_folder_id: str, user_id: int, context: ContextTypes.DEFAULT_TYPE):
    try:
        # Create destination folder
        src_folder = service.files().get(
            fileId=src_folder_id,
            fields='name, mimeType'
        ).execute()
        
        dest_folder = service.files().create(body={
            'name': src_folder['name'],
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [dest_folder_id] if dest_folder_id else []
        }).execute()
        
        # Process folder contents
        page_token = None
        while True:
            response = service.files().list(
                q=f"'{src_folder_id}' in parents",
                fields="nextPageToken, files(id, name, mimeType, size)",
                pageToken=page_token
            ).execute()
            
            for file in response.get('files', []):
                # Update progress
                progress_data[user_id]['processed_files'] += 1
                progress_data[user_id]['file_types'][categorize_file(file['mimeType'])] += 1
                
                # Update progress every 10 files or when 5% changes
                if progress_data[user_id]['processed_files'] % 10 == 0:
                    await update_progress_ui(context, user_id)
                
                # Copy file/folder
                if file['mimeType'] == 'application/vnd.google-apps.folder':
                    await copy_folder(service, file['id'], dest_folder['id'], user_id, context)
                else:
                    service.files().copy(
                        fileId=file['id'],
                        body={'parents': [dest_folder['id']]}
                    ).execute()
            
            page_token = response.get('nextPageToken')
            if not page_token:
                break
        
        return dest_folder['id']
    
    except HttpError as e:
        logger.error(f'Drive API Error: {e}')
        raise

async def update_progress_ui(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    data = progress_data.get(user_id, {})
    if not data:
        return
    
    progress = (data['processed_files'] / data['total_files']) * 100 if data['total_files'] > 0 else 0
    message = (
        f"📁 Copy Progress: {progress:.1f}%\n"
        f"📦 Total Size: {format_size(data['total_size'])}\n"
        f"📊 File Statistics:\n{format_file_types(data['file_types'])}"
    )
    
    try:
        await context.bot.edit_message_text(
            chat_id=user_id,
            message_id=data['message_id'],
            text=message
        )
    except Exception as e:
        logger.error(f"Error updating progress: {e}")

def format_size(size_bytes: int) -> str:
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    size = float(size_bytes)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.2f} {unit}"
        size /= 1024

def format_file_types(file_types: dict) -> str:
    return '\n'.join([f"• {k}: {v}" for k, v in file_types.items() if v > 0])

def categorize_file(mime_type: str) -> str:
    for pattern, category in FILE_TYPES.items():
        if mime_type.startswith(pattern):
            return category
    return 'Other'

async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    if user_id in progress_data:
        del progress_data[user_id]
    await update.message.reply_text("✅ Current operation cancelled.")

def main():
    app = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Conversation handler for authorization
    auth_conv = ConversationHandler(
        entry_points=[CommandHandler('auth', start_auth)],
        states={
            'WAITING_FOR_CODE': [CommandHandler('code', handle_code)],
        },
        fallbacks=[CommandHandler('cancel', cancel_auth)],
    )
    
    # Add handlers
    app.add_handler(CommandHandler('start', start))
    app.add_handler(auth_conv)
    app.add_handler(CommandHandler('revoke', revoke_auth))
    app.add_handler(CommandHandler('cancel', cancel))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_drive_link))
    
    # Start bot
    app.run_polling()

if __name__ == '__main__':
    main()