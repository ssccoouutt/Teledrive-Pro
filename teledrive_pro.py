import os
import logging
import re
import io
import tempfile
from collections import defaultdict
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
    ConversationHandler,
    CallbackQueryHandler
)
from telegram.constants import ChatMemberStatus
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

# Configuration
SCOPES = ['https://www.googleapis.com/auth/drive']
TELEGRAM_BOT_TOKEN = '7404351306:AAHiqgrn0r1uctvPfB1yNyns5qHcMYqatp4'
GOOGLE_DRIVE_FOLDER_ID = '1GLGkmpm-_h0dw-lc0mmJE2i1Ma0YrAKm'
ADMIN_USER_ID = 990321391
WHATSAPP_LINK = "https://wa.me/923247220362"
REQUIRED_CHANNEL = '@TechZoneX'  # Channel username with @
PREMIUM_FILE_ID = '1726HMqaHlLgiOpvjIeqkOMCq0zrTwitR'
ACTIVITY_FILE_ID = '1621J8IK0m98fVgxNqdLSuRYlJydI1PjY'

# Updated Plan Limits
PLAN_LIMITS = {
    'free': {
        'daily': 1,
        'size': 2 * 1024**3,  # 2GB
        'files': 20,           # 20 files
        'duration': 'per day'
    },
    'basic': {
        'daily': 10,
        'size': 20 * 1024**3,  # 20GB
        'files': 150,           # 150 files
        'duration': '1 week'
    },
    'premium': {
        'daily': 30,
        'size': 100 * 1024**3,  # 100GB
        'files': 500,            # 500 files
        'duration': '1 week'
    }
}

# Pricing Information
PRICING = {
    'basic': {'PKR': 350, 'USD': 2},
    'premium': {'PKR': 500, 'USD': 3}
}

# Payment Methods
PAYMENT_METHODS = ["Easypaisa", "Jazzcash", "Binance"]

# Contact Text with updated WhatsApp link
CONTACT_TEXT = "🔗 Contact [@itszeeshan196](tg://user?id=990321391) or [WhatsApp]({}) to upgrade".format(WHATSAPP_LINK)

# Initialize logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Global variables
pending_authorizations = {}
progress_data = defaultdict(lambda: {
    'total_files': 0,
    'processed_files': 0,
    'file_types': defaultdict(int),
    'total_size': 0,
    'message_id': None,
    'chat_id': None
})
user_usage = defaultdict(lambda: {'count': 0, 'last_used': None})
drive_service = None
PREMIUM_USERS = set()
BASIC_USERS = set()

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

def get_credentials_file_from_drive(service, filename):
    """Download credentials file from Google Drive folder."""
    try:
        results = service.files().list(
            q=f"'{GOOGLE_DRIVE_FOLDER_ID}' in parents and name='{filename}'",
            pageSize=1,
            fields="files(id, name)"
        ).execute()
        items = results.get('files', [])
        
        if not items:
            return None
            
        file_id = items[0]['id']
        request = service.files().get_media(fileId=file_id)
        file_content = request.execute()
        
        temp_dir = tempfile.gettempdir()
        file_path = os.path.join(temp_dir, filename)
        
        with open(file_path, 'wb') as f:
            f.write(file_content)
            
        return file_path
    except Exception as e:
        logger.error(f"Error getting {filename} from Drive: {e}")
        return None

def save_token_to_drive(creds):
    """Save token to Google Drive folder."""
    try:
        token_content = creds.to_json()
        
        results = drive_service.files().list(
            q=f"'{GOOGLE_DRIVE_FOLDER_ID}' in parents and name='token.json'",
            pageSize=1,
            fields="files(id)"
        ).execute()
        items = results.get('files', [])
        
        media = MediaIoBaseUpload(
            io.BytesIO(token_content.encode('utf-8')),
            mimetype='application/json'
        )
        
        if items:
            drive_service.files().update(
                fileId=items[0]['id'],
                media_body=media
            ).execute()
        else:
            drive_service.files().create(
                body={
                    'name': 'token.json',
                    'parents': [GOOGLE_DRIVE_FOLDER_ID]
                },
                media_body=media
            ).execute()
    except Exception as e:
        logger.error(f"Error saving token to Drive: {e}")

def initialize_drive_service():
    """Initialize Drive service using credentials from Google Drive."""
    global drive_service
    creds = None
    
    try:
        # Create minimal service to access Drive folder
        temp_service = build('drive', 'v3', credentials=None)
        
        # Get credentials.json from Drive
        creds_file = get_credentials_file_from_drive(temp_service, 'credentials.json')
        if not creds_file:
            logger.error("Could not download credentials.json from Drive")
            return
            
        # Get token.json from Drive if exists
        token_file = get_credentials_file_from_drive(temp_service, 'token.json')
        
        if token_file:
            creds = Credentials.from_authorized_user_file(token_file, SCOPES)
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
                save_token_to_drive(creds)
        
        if not creds or not creds.valid:
            flow = Flow.from_client_secrets_file(
                creds_file,
                scopes=SCOPES,
                redirect_uri='http://localhost:8080'
            )
            auth_url, _ = flow.authorization_url(prompt='consent')
            logger.info(f"Admin needs to authenticate via: {auth_url}")
            return
            
        drive_service = build('drive', 'v3', credentials=creds)
        load_subscribed_users()
        logger.info("Drive service initialized successfully")
        
    except Exception as e:
        logger.error(f"Error initializing Drive service: {e}")

def load_subscribed_users():
    """Load users from Google Drive file."""
    global PREMIUM_USERS, BASIC_USERS
    premium = set()
    basic = set()
    current_section = 'premium'
    
    try:
        if not drive_service:
            logger.warning("Drive service not initialized - using empty user lists")
            PREMIUM_USERS = premium
            BASIC_USERS = basic
            return
            
        request = drive_service.files().get_media(fileId=PREMIUM_FILE_ID)
        content = request.execute().decode('utf-8')
        
        for line in content.split('\n'):
            line = line.strip()
            if not line:
                current_section = 'basic'
                continue
            if line.isdigit():
                user_id = int(line)
                if current_section == 'premium':
                    premium.add(user_id)
                else:
                    basic.add(user_id)
    except Exception as e:
        logger.error(f"Error loading users: {e}")
    
    PREMIUM_USERS = premium
    BASIC_USERS = basic

def save_subscribed_users():
    """Save users to Google Drive file."""
    if not drive_service:
        logger.warning("Cannot save users - Drive service not initialized")
        return False
    
    content = []
    # Premium users
    content.extend(str(uid) for uid in PREMIUM_USERS)
    # Separator
    content.append('')
    # Basic users
    content.extend(str(uid) for uid in BASIC_USERS)
    
    media = MediaIoBaseUpload(
        io.BytesIO('\n'.join(content).encode('utf-8')),
        mimetype='text/plain'
    )
    
    try:
        drive_service.files().update(
            fileId=PREMIUM_FILE_ID,
            media_body=media
        ).execute()
        return True
    except Exception as e:
        logger.error(f"Error saving users: {e}")
        return False

def save_activity_log(user_id: int, username: str, first_name: str, link: str):
    """Save user activity to Google Drive file."""
    if not drive_service:
        logger.info("Skipping activity log - Drive service not initialized")
        return False
    
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    entry = f"{timestamp} | User ID: {user_id} | Username: @{username} | Name: {first_name} | Link: {link}\n"
    
    try:
        # Get existing content
        request = drive_service.files().get_media(fileId=ACTIVITY_FILE_ID)
        existing_content = request.execute().decode('utf-8')
    except:
        existing_content = ""
    
    # Add new entry at the top
    new_content = entry + existing_content
    
    media = MediaIoBaseUpload(
        io.BytesIO(new_content.encode('utf-8')),
        mimetype='text/plain'
    )
    
    try:
        drive_service.files().update(
            fileId=ACTIVITY_FILE_ID,
            media_body=media
        ).execute()
        return True
    except Exception as e:
        logger.error(f"Error saving activity log: {e}")
        return False

def get_user_token_path(user_id: int) -> str:
    """Get path for user token in Railway's temp storage."""
    return os.path.join(tempfile.gettempdir(), f'token_{user_id}.json')

async def check_channel_membership(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Check if user is member of required channel."""
    try:
        user = update.effective_user
        if user.id == ADMIN_USER_ID:
            return True
            
        member = await context.bot.get_chat_member(REQUIRED_CHANNEL, user.id)
        if member.status in [ChatMemberStatus.LEFT, ChatMemberStatus.BANNED]:
            keyboard = [
                [InlineKeyboardButton("Join Channel", url=f"https://t.me/{REQUIRED_CHANNEL[1:]}")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await context.bot.send_message(
                chat_id=user.id,
                text=f"⚠️ Please join {REQUIRED_CHANNEL} first",
                parse_mode='Markdown',
                reply_markup=reply_markup
            )
            return False
        return True
    except Exception as e:
        logger.error(f"Error checking channel membership: {e}")
        return True

async def auth_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /auth command for admin Google Drive authorization."""
    user = update.effective_user
    
    if user.id != ADMIN_USER_ID:
        await update.message.reply_text("❌ Admin only command", parse_mode='Markdown')
        return
    
    token_path = os.path.join(tempfile.gettempdir(), 'token.json')
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        if creds and creds.valid:
            await update.message.reply_text("✅ Already authorized", parse_mode='Markdown')
            return
    
    try:
        temp_service = build('drive', 'v3', credentials=None)
        creds_file = get_credentials_file_from_drive(temp_service, 'credentials.json')
        if not creds_file:
            await update.message.reply_text("❌ Missing credentials in Drive", parse_mode='Markdown')
            return
            
        flow = Flow.from_client_secrets_file(
            creds_file,
            scopes=SCOPES,
            redirect_uri='http://localhost:8080'
        )
        auth_url, _ = flow.authorization_url(prompt='consent')
        pending_authorizations[ADMIN_USER_ID] = flow
        
        await update.message.reply_text(
            f"🔑 [Authorize Google Drive]({auth_url})\n\n"
            "After approving, send the redirect URL",
            parse_mode='Markdown',
            disable_web_page_preview=True,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data='cancel_admin_auth')]
            ])
        )
        return 'WAITING_FOR_ADMIN_CODE'
    except Exception as e:
        await update.message.reply_text(f"❌ Error: {str(e)}", parse_mode='Markdown')
        return ConversationHandler.END

async def handle_admin_auth_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process admin authorization code from redirect URL."""
    user_id = update.message.from_user.id
    text = update.message.text.strip()
    auth_code = extract_auth_code(text)
    
    if not auth_code or user_id not in pending_authorizations:
        await update.message.reply_text("❌ Invalid URL", parse_mode='Markdown')
        return ConversationHandler.END
    
    try:
        flow = pending_authorizations[user_id]
        flow.fetch_token(code=auth_code)
        creds = flow.credentials
        
        # Save token to Drive
        save_token_to_drive(creds)
        
        # Also save locally for current session
        with open(os.path.join(tempfile.gettempdir(), 'token.json'), 'w') as token_file:
            token_file.write(creds.to_json())
        
        del pending_authorizations[user_id]
        
        global drive_service
        drive_service = build('drive', 'v3', credentials=creds)
        load_subscribed_users()
        
        await update.message.reply_text(
            "✅ Admin authorization successful!",
            parse_mode='Markdown'
        )
    except Exception as e:
        await update.message.reply_text(
            f"❌ Authorization failed: {str(e)}",
            parse_mode='Markdown'
        )
    finally:
        return ConversationHandler.END

async def cancel_admin_auth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel admin authorization process."""
    query = update.callback_query
    await query.answer()
    
    if ADMIN_USER_ID in pending_authorizations:
        del pending_authorizations[ADMIN_USER_ID]
    
    await query.edit_message_text("❌ Authorization cancelled", parse_mode='Markdown')
    return ConversationHandler.END

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send welcome message."""
    if not await check_channel_membership(update, context):
        return
    
    query = update.callback_query
    user_id = update.message.from_user.id if update.message else query.from_user.id
    current_plan = 'premium' if user_id in PREMIUM_USERS else 'basic' if user_id in BASIC_USERS else 'free'
    
    plan_status = ""
    if current_plan != 'free':
        plan_status = f"\n\n✨ Your Plan: {current_plan.capitalize()}"
    
    keyboard = [
        [InlineKeyboardButton("🔑 Connect Drive", callback_data='start_auth')],
        [InlineKeyboardButton("📊 View Plans", callback_data='show_plans')],
        [InlineKeyboardButton("🛠 Help", callback_data='help')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    welcome_msg = (
        "🚀 Welcome to Google Drive Manager Bot!\n\n"
        "I can help you copy Drive folders to your account."
        f"{plan_status}\n\n"
        f"{CONTACT_TEXT}"
    )
    
    if update.message:
        await update.message.reply_text(
            welcome_msg,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    else:
        await query.edit_message_text(
            welcome_msg,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )

async def show_plans(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show available plans."""
    if not await check_channel_membership(update, context):
        return
    
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    current_plan = 'premium' if user_id in PREMIUM_USERS else 'basic' if user_id in BASIC_USERS else 'free'
    
    plan_status = ""
    if current_plan != 'free':
        plan_status = f"\n\n✨ Your Plan: {current_plan.capitalize()}"
    
    keyboard = [
        [InlineKeyboardButton("🆓 Free", callback_data='plan_free')],
        [InlineKeyboardButton("⭐ Basic", callback_data='plan_basic')],
        [InlineKeyboardButton("💎 Premium", callback_data='plan_premium')],
        [InlineKeyboardButton("💳 Payments", callback_data='payment_methods')],
        [InlineKeyboardButton("🔙 Back", callback_data='main_menu')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"📊 Available Plans{plan_status}\n\n"
        "Choose a plan to view details:",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def show_payment_methods(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show payment methods."""
    if not await check_channel_membership(update, context):
        return
    
    query = update.callback_query
    await query.answer()
    
    payment_text = "💳 Payment Methods:\n\n"
    for method in PAYMENT_METHODS:
        payment_text += f"• {method}\n"
    payment_text += f"\n{CONTACT_TEXT}"
    
    keyboard = [
        [InlineKeyboardButton("🔙 Back", callback_data='show_plans')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        payment_text,
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle button presses."""
    if not await check_channel_membership(update, context):
        return
    
    query = update.callback_query
    await query.answer()
    
    if query.data == 'start_auth':
        await start_auth(update, context)
    elif query.data == 'show_plans':
        await show_plans(update, context)
    elif query.data.startswith('plan_'):
        await plan_details(update, context)
    elif query.data == 'payment_methods':
        await show_payment_methods(update, context)
    elif query.data == 'main_menu':
        await start(update, context)
    elif query.data == 'help':
        await help_command(update, context)
    elif query.data == 'confirm_delete':
        await delete_confirmed(update, context)
    elif query.data == 'cancel_delete':
        await cancel_delete(update, context)
    elif query.data == 'cancel_auth':
        await cancel_auth(update, context)
    elif query.data == 'cancel_admin_auth':
        await cancel_admin_auth(update, context)
    else:
        try:
            await query.message.delete()
        except:
            pass

async def plan_details(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show plan details."""
    query = update.callback_query
    await query.answer()
    plan = query.data.split('_')[1]
    
    emoji = {
        'free': '🆓',
        'basic': '⭐',
        'premium': '💎'
    }.get(plan, '📊')
    
    details = [
        f"{emoji} {plan.capitalize()} Plan:\n\n",
        f"• Folders/day: {PLAN_LIMITS[plan]['daily']}\n",
        f"• Max size: {format_size(PLAN_LIMITS[plan]['size'])}\n",
        f"• Max files: {PLAN_LIMITS[plan]['files']}\n",
        f"• Duration: {PLAN_LIMITS[plan]['duration']}\n\n"
    ]
    
    if plan != 'free':
        details.extend([
            f"💵 Pricing:\n",
            f"• PKR {PRICING[plan]['PKR']}\n",
            f"• USD {PRICING[plan]['USD']}\n\n"
        ])
    
    details.append(CONTACT_TEXT)
    
    keyboard = [
        [InlineKeyboardButton("🔙 Back", callback_data='show_plans')],
        [InlineKeyboardButton("🔼 Upgrade", url=WHATSAPP_LINK)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        ''.join(details),
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show help message."""
    if not await check_channel_membership(update, context):
        return
    
    user = update.effective_user
    is_admin = user.id == ADMIN_USER_ID
    
    help_text = [
        "🛠 Commands:\n\n",
        "• /start - Start bot\n",
        "• /delete - Remove authorization\n"
    ]
    
    if is_admin:
        help_text.extend([
            "\n👑 Admin Commands:\n",
            "• /auth - Configure Drive access\n",
            "• /add [user_id] [basic|premium]\n",
            "• /remove [user_id|all]\n"
        ])
    
    help_text.append(f"\n{CONTACT_TEXT}")
    
    keyboard = [[InlineKeyboardButton("🔙 Back", callback_data='main_menu')]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    if update.message:
        await update.message.reply_text(
            ''.join(help_text),
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
    else:
        await query.edit_message_text(
            ''.join(help_text),
            parse_mode='Markdown',
            reply_markup=reply_markup
        )

async def delete_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle delete command."""
    if not await check_channel_membership(update, context):
        return
    
    keyboard = [
        [
            InlineKeyboardButton("✅ Confirm", callback_data='confirm_delete'),
            InlineKeyboardButton("❌ Cancel", callback_data='cancel_delete')
        ]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "⚠️ Confirm authorization removal?",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def delete_confirmed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle confirmed deletion."""
    query = update.callback_query
    user_id = query.from_user.id
    token_path = get_user_token_path(user_id)
    
    try:
        await query.message.delete()
    except:
        pass
    
    if os.path.exists(token_path):
        os.remove(token_path)
        msg = "✅ Authorization removed"
    else:
        msg = "ℹ️ No authorization found"
    
    await context.bot.send_message(
        chat_id=user_id,
        text=msg,
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Menu", callback_data='main_menu')]
        ])
    )

async def cancel_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel deletion."""
    query = update.callback_query
    await query.answer()
    
    try:
        await query.message.delete()
    except:
        pass
    
    await context.bot.send_message(
        chat_id=query.from_user.id,
        text="✅ Deletion cancelled",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Menu", callback_data='main_menu')]
        ])
    )

async def start_auth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start user authorization."""
    if not await check_channel_membership(update, context):
        return
    
    query = update.callback_query
    user_id = query.from_user.id
    token_path = get_user_token_path(user_id)
    
    if os.path.exists(token_path):
        await query.edit_message_text(
            "🔒 Already authorized\n\nUse /delete to remove",
            parse_mode='Markdown'
        )
        return ConversationHandler.END
    
    try:
        temp_service = build('drive', 'v3', credentials=None)
        creds_file = get_credentials_file_from_drive(temp_service, 'credentials.json')
        if not creds_file:
            await query.edit_message_text("❌ Configuration error", parse_mode='Markdown')
            return ConversationHandler.END
            
        flow = Flow.from_client_secrets_file(
            creds_file,
            scopes=SCOPES,
            redirect_uri='http://localhost:8080'
        )
        auth_url, _ = flow.authorization_url(prompt='consent')
        pending_authorizations[user_id] = flow
        
        await query.edit_message_text(
            f"🔑 [Authorize Google Drive]({auth_url})\n\n"
            "After approving, send the redirect URL",
            parse_mode='Markdown',
            disable_web_page_preview=True,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("❌ Cancel", callback_data='cancel_auth')]
            ])
        )
        return 'WAITING_FOR_CODE'
    except Exception as e:
        await query.edit_message_text(f"❌ Error: {str(e)}", parse_mode='Markdown')
        return ConversationHandler.END

async def handle_auth_code(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Process user authorization code."""
    user_id = update.message.from_user.id
    text = update.message.text.strip()
    auth_code = extract_auth_code(text)
    
    if not auth_code or user_id not in pending_authorizations:
        await update.message.reply_text("❌ Invalid URL", parse_mode='Markdown')
        return ConversationHandler.END
    
    try:
        flow = pending_authorizations[user_id]
        flow.fetch_token(code=auth_code)
        creds = flow.credentials
        
        with open(get_user_token_path(user_id), 'w') as token_file:
            token_file.write(creds.to_json())
        
        del pending_authorizations[user_id]
        
        await update.message.reply_text(
            "✅ Authorization successful!",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📊 Plans", callback_data='show_plans')],
                [InlineKeyboardButton("🛠 Help", callback_data='help')]
            ])
        )
    except Exception as e:
        await update.message.reply_text(
            f"❌ Authorization failed: {str(e)}",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Retry", callback_data='start_auth')]
            ])
        )
    finally:
        return ConversationHandler.END

async def cancel_auth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel user authorization."""
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    
    if user_id in pending_authorizations:
        del pending_authorizations[user_id]
    
    await query.edit_message_text(
        "❌ Authorization cancelled",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Menu", callback_data='main_menu')]
        ])
    )
    return ConversationHandler.END

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle incoming messages."""
    if not await check_channel_membership(update, context):
        return
    
    user_id = update.message.from_user.id
    text = update.message.text.strip()
    
    # Check for auth code
    auth_code = extract_auth_code(text)
    if auth_code:
        if user_id in pending_authorizations:
            return await handle_auth_code(update, context, auth_code)
        elif user_id == ADMIN_USER_ID and ADMIN_USER_ID in pending_authorizations:
            return await handle_admin_auth_code(update, context)
    
    # Check for Drive link
    if 'drive.google.com' in text:
        return await handle_drive_link(update, context)
    
    # Default response
    await update.message.reply_text(
        "Send a Drive folder link or use the buttons below:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔑 Connect Drive", callback_data='start_auth')],
            [InlineKeyboardButton("📊 Plans", callback_data='show_plans')]
        ])
    )

async def handle_drive_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Drive folder links."""
    if not await check_channel_membership(update, context):
        return
    
    user = update.message.from_user
    user_id = user.id
    user_tier = 'premium' if user_id in PREMIUM_USERS else 'basic' if user_id in BASIC_USERS else 'free'
    
    # Save activity log if admin auth exists
    if drive_service and user_id != ADMIN_USER_ID:
        save_activity_log(
            user_id,
            user.username or "no_username",
            user.first_name or "No Name",
            update.message.text
        )
    
    # Check daily limit
    today = datetime.now().date()
    if user_usage[user_id]['last_used'] != today:
        user_usage[user_id] = {'count': 0, 'last_used': today}
    
    if user_usage[user_id]['count'] >= PLAN_LIMITS[user_tier]['daily']:
        await update.message.reply_text(
            f"❌ Daily limit reached ({PLAN_LIMITS[user_tier]['daily']} folders)",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Upgrade", url=WHATSAPP_LINK)]
            ])
        )
        return
    
    # Check user authorization
    creds = None
    token_path = get_user_token_path(user_id)
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
            with open(token_path, 'w') as token:
                token.write(creds.to_json())
    
    if not creds or not creds.valid:
        await update.message.reply_text(
            "❌ Please authorize Google Drive first",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Authorize", callback_data='start_auth')]
            ])
        )
        return
    
    try:
        # Extract folder ID
        folder_id = re.search(r'/folders/([a-zA-Z0-9_-]+)', update.message.text).group(1)
        service = build('drive', 'v3', credentials=creds)
        
        # Check folder size and file count
        total_files, total_size = count_files_and_size(service, folder_id)
        if total_size > PLAN_LIMITS[user_tier]['size']:
            await update.message.reply_text(
                f"❌ Folder too large ({format_size(total_size)} > {format_size(PLAN_LIMITS[user_tier]['size'])})",
                parse_mode='Markdown'
            )
            return
        if total_files > PLAN_LIMITS[user_tier]['files']:
            await update.message.reply_text(
                f"❌ Too many files ({total_files} > {PLAN_LIMITS[user_tier]['files']})",
                parse_mode='Markdown'
            )
            return
        
        # Update usage and start copying
        user_usage[user_id]['count'] += 1
        progress_msg = await update.message.reply_text("⏳ Starting copy...", parse_mode='Markdown')
        
        progress_data[user_id] = {
            'total_files': total_files,
            'processed_files': 0,
            'file_types': defaultdict(int),
            'total_size': total_size,
            'message_id': progress_msg.message_id,
            'chat_id': update.message.chat_id
        }
        
        context.job_queue.run_once(
            lambda ctx: copy_folder_process(ctx, user_id, folder_id),
            0,
            data={'chat_id': update.message.chat_id}
        )
        
    except Exception as e:
        await update.message.reply_text(
            f"❌ Error: {str(e)}",
            parse_mode='Markdown'
        )

async def copy_folder_process(context: ContextTypes.DEFAULT_TYPE, user_id: int, folder_id: str):
    """Copy folder and update progress."""
    job = context.job
    chat_id = job.data['chat_id']
    
    try:
        creds = None
        token_path = get_user_token_path(user_id)
        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        
        if not creds or not creds.valid:
            await context.bot.send_message(
                chat_id=chat_id,
                text="❌ Authorization expired",
                parse_mode='Markdown'
            )
            return
            
        service = build('drive', 'v3', credentials=creds)
        
        await update_progress(context, user_id, "🔍 Analyzing folder...")
        total_files, total_size = count_files_and_size(service, folder_id)
        
        progress_data[user_id].update({
            'total_files': total_files,
            'total_size': total_size
        })
        
        await update_progress(context, user_id, "🚀 Copying files...")
        await copy_folder(service, folder_id, None, user_id, context)
        
        # Completion message
        success_msg = (
            f"✅ Copy complete!\n\n"
            f"📂 Files: `{progress_data[user_id]['processed_files']}`\n"
            f"📦 Size: `{format_size(progress_data[user_id]['total_size'])}`\n"
            f"📊 File types:\n{format_file_types(progress_data[user_id]['file_types'])}"
        )
        
        await context.bot.send_message(
            chat_id=chat_id,
            text=success_msg,
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("📊 Plans", callback_data='show_plans')],
                [InlineKeyboardButton("🔙 Menu", callback_data='main_menu')]
            ])
        )
        
    except Exception as e:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"❌ Error: {str(e)}",
            parse_mode='Markdown'
        )
    finally:
        if user_id in progress_data:
            try:
                await context.bot.delete_message(
                    chat_id=chat_id,
                    message_id=progress_data[user_id]['message_id']
                )
            except:
                pass
            del progress_data[user_id]

async def update_progress(context: ContextTypes.DEFAULT_TYPE, user_id: int, message: str):
    """Update progress message."""
    if user_id not in progress_data:
        return
        
    data = progress_data[user_id]
    chat_id = data.get('chat_id')
    message_id = data.get('message_id')
    
    if not chat_id or not message_id:
        return
    
    try:
        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=message_id,
            text=message,
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.warning(f"Failed to update progress: {e}")

async def copy_folder(service, src_folder_id: str, dest_folder_id: str, user_id: int, context: ContextTypes.DEFAULT_TYPE):
    """Recursively copy folder contents."""
    try:
        src_folder = service.files().get(fileId=src_folder_id).execute()
        dest_folder = service.files().create(body={
            'name': src_folder['name'],
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [dest_folder_id] if dest_folder_id else []
        }).execute()
        
        page_token = None
        while True:
            response = service.files().list(
                q=f"'{src_folder_id}' in parents",
                fields="nextPageToken, files(id, name, mimeType, size)",
                pageToken=page_token
            ).execute()
            
            for file in response.get('files', []):
                progress_data[user_id]['processed_files'] += 1
                file_type = categorize_file(file['mimeType'])
                progress_data[user_id]['file_types'][file_type] += 1
                
                if progress_data[user_id]['processed_files'] % 10 == 0:
                    await update_progress_ui(context, user_id)
                
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
        logger.error(f"Drive API error: {e}")
        raise

async def update_progress_ui(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Update progress UI."""
    data = progress_data.get(user_id, {})
    if not data:
        return
    
    progress = (data['processed_files'] / data['total_files']) * 100 if data['total_files'] > 0 else 0
    message = (
        f"📁 Progress: `{progress:.1f}%`\n"
        f"📦 Size: `{format_size(data['total_size'])}`\n"
        f"📊 File types:\n{format_file_types(data['file_types'])}"
    )
    
    await update_progress(context, user_id, message)

def count_files_and_size(service, folder_id: str) -> tuple:
    """Count files and calculate total size in folder."""
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

def format_size(size_bytes: int) -> str:
    """Convert bytes to human-readable format."""
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    size = float(size_bytes)
    
    for unit in units:
        if size < 1024 or unit == units[-1]:
            return f"{size:.2f} {unit}"
        size /= 1024

def format_file_types(file_types: dict) -> str:
    """Format file types for display."""
    return '\n'.join([f"• *{k}:* `{v}`" for k, v in file_types.items() if v > 0])

def categorize_file(mime_type: str) -> str:
    """Categorize file by MIME type."""
    for pattern, category in FILE_TYPES.items():
        if mime_type.startswith(pattern):
            return category
    return 'Other'

def extract_auth_code(url: str) -> str:
    """Extract auth code from redirect URL."""
    parsed = urlparse(url)
    if parsed.netloc in ['localhost', 'localhost:8080']:
        query = parse_qs(parsed.query)
        return query.get('code', [None])[0]
    return None

async def reload_users(context: ContextTypes.DEFAULT_TYPE):
    """Reload user lists periodically."""
    global PREMIUM_USERS, BASIC_USERS
    load_subscribed_users()
    logger.info("Reloaded user lists")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle errors."""
    logger.error(f"Error: {context.error}", exc_info=context.error)
    
    try:
        await context.bot.send_message(
            chat_id=ADMIN_USER_ID,
            text=f"Error: {context.error}\nUpdate: {update}"
        )
    except:
        pass

def main():
    """Start the bot."""
    # Initialize logging
    logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO
    )
    logger = logging.getLogger(__name__)

    try:
        # Initialize Drive service
        initialize_drive_service()

        # Create application
        application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()

        # Admin auth conversation
        admin_auth_conv = ConversationHandler(
            entry_points=[CommandHandler('auth', auth_command)],
            states={
                'WAITING_FOR_ADMIN_CODE': [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_admin_auth_code)
                ]
            },
            fallbacks=[
                CallbackQueryHandler(cancel_admin_auth, pattern='^cancel_admin_auth$'),
                CommandHandler('cancel', cancel_admin_auth)
            ]
        )

        # User auth conversation
        user_auth_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(start_auth, pattern='^start_auth$')],
            states={
                'WAITING_FOR_CODE': [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_auth_code)
                ]
            },
            fallbacks=[
                CallbackQueryHandler(cancel_auth, pattern='^cancel_auth$'),
                CommandHandler('cancel', cancel_auth)
            ],
            per_message=True
        )

        # Command handlers
        command_handlers = [
            CommandHandler('start', start),
            CommandHandler('help', help_command),
            CommandHandler('delete', delete_command),
            CommandHandler('add', add_user_command),
            CommandHandler('remove', remove_user_command)
        ]

        # Add all handlers
        for handler in command_handlers:
            application.add_handler(handler)
        
        application.add_handler(CallbackQueryHandler(button_handler))
        application.add_handler(admin_auth_conv)
        application.add_handler(user_auth_conv)
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
        application.add_error_handler(error_handler)

        # Scheduled job to reload users
        application.job_queue.run_repeating(reload_users, interval=300, first=10)

        # Start bot
        logger.info("Starting bot...")
        application.run_polling(
            poll_interval=1,
            timeout=10,
            drop_pending_updates=True,
            allowed_updates=Update.ALL_TYPES
        )

    except Exception as e:
        logger.critical(f"Fatal error: {e}", exc_info=True)
    finally:
        logger.info("Bot stopped")

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\nBot stopped by user")
    except Exception as e:
        logging.critical(f"Unhandled exception: {e}", exc_info=True)
        raise