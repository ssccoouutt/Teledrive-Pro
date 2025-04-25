import os
import logging
import re
import io
import asyncio
import aiohttp
import time
import random
import signal
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
    CallbackQueryHandler,
    JobQueue
)
from telegram.constants import ChatMemberStatus
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload
from aiohttp import web

# Configuration
SCOPES = ['https://www.googleapis.com/auth/drive']
TELEGRAM_BOT_TOKEN = '7404351306:AAHiqgrn0r1uctvPfB1yNyns5qHcMYqatp4'
CLIENT_SECRET_FILE = 'credentials.json'
TOKENS_FOLDER_ID = '1IYg1eoDjJPbmQtnLDOwKawGobMZsV9kF'  # Your Google Drive folder for user tokens
PREMIUM_FILE_ID = '1KcfqPvm-4WWxQqFcEibo07fh6TfzM4KL'
ADMIN_USER_ID = 990321391
WHATSAPP_LINK = "https://wa.me/923247220362"
ACTIVITY_FILE_ID = '1L0vKicGiJKMAww11bVutpjg2qbrQ5pC5'
REQUIRED_CHANNEL = '@TechZoneX'

# Web Server Configuration
WEB_PORT = 8000
PING_INTERVAL = 25
HEALTH_CHECK_ENDPOINT = "/health"

# Plan Limits
PLAN_LIMITS = {
    'free': {
        'daily': 1,
        'size': 2 * 1024**3,
        'files': 20,
        'duration': 'per day'
    },
    'basic': {
        'daily': 10,
        'size': 20 * 1024**3,
        'files': 150,
        'duration': '1 week'
    },
    'premium': {
        'daily': 30,
        'size': 100 * 1024**3,
        'files': 500,
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

# Contact Text
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
runner = None
site = None

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

async def health_check(request):
    """Health check endpoint for Koyeb"""
    return web.Response(
        text=f"🤖 Bot is operational | Last active: {datetime.now()}",
        headers={"Content-Type": "text/plain"},
        status=200
    )

async def root_handler(request):
    """Root endpoint handler for Koyeb health checks"""
    return web.Response(
        text="Bot is running",
        status=200
    )

async def self_ping():
    """Keep-alive mechanism for Koyeb"""
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f'http://localhost:{WEB_PORT}{HEALTH_CHECK_ENDPOINT}') as resp:
                    status = f"Status: {resp.status}" if resp.status != 200 else "Success"
                    logger.info(f"Keepalive ping {status}")
                    
            with open('/tmp/last_active.txt', 'w') as f:
                f.write(str(datetime.now()))
                
        except Exception as e:
            logger.error(f"Keepalive error: {str(e)}")
        
        await asyncio.sleep(PING_INTERVAL)

async def run_webserver():
    """Run the web server for health checks"""
    app = web.Application()
    app.router.add_get(HEALTH_CHECK_ENDPOINT, health_check)
    app.router.add_get("/", root_handler)
    
    global runner, site
    runner = web.AppRunner(app)
    await runner.setup()
    
    site = web.TCPSite(runner, '0.0.0.0', WEB_PORT)
    await site.start()
    logger.info(f"Health check server running on port {WEB_PORT}")

def initialize_drive_service():
    """Initialize Drive service with owner's token from local file"""
    global drive_service
    max_retries = 3
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            # Load token.json from local file
            if os.path.exists('token.json'):
                with open('token.json', 'r') as token_file:
                    token_content = token_file.read()
                    creds = Credentials.from_authorized_user_info(eval(token_content), SCOPES)
                    
                    if creds and creds.expired and creds.refresh_token:
                        creds.refresh(Request())
                    
                    if creds and creds.valid:
                        drive_service = build('drive', 'v3', credentials=creds)
                        load_subscribed_users()
                        logger.info("Drive service initialized successfully with owner token")
                        return
            else:
                logger.error("token.json not found in root directory")
                
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed to initialize Drive service: {str(e)}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
    
    logger.error("Failed to initialize Drive service after multiple attempts")

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
    content.extend(str(uid) for uid in PREMIUM_USERS)
    content.append('')
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
        request = drive_service.files().get_media(fileId=ACTIVITY_FILE_ID)
        existing_content = request.execute().decode('utf-8')
    except:
        existing_content = ""
    
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

async def get_user_token_from_drive(user_id: int):
    """Get user token from Google Drive folder."""
    if not drive_service:
        return None
    
    try:
        # Search for user token file in the tokens folder
        query = f"'{TOKENS_FOLDER_ID}' in parents and name = 'token_{user_id}.json'"
        response = drive_service.files().list(
            q=query,
            fields='files(id)'
        ).execute()
        
        files = response.get('files', [])
        if files:
            file_id = files[0]['id']
            request = drive_service.files().get_media(fileId=file_id)
            token_content = request.execute().decode('utf-8')
            return Credentials.from_authorized_user_info(eval(token_content), SCOPES)
    except Exception as e:
        logger.error(f"Error getting user token from Drive: {e}")
    
    return None

async def save_user_token_to_drive(user_id: int, creds: Credentials):
    """Save user token to Google Drive folder."""
    if not drive_service:
        return False
    
    try:
        token_content = creds.to_json()
        file_name = f'token_{user_id}.json'
        
        # Check if file already exists
        query = f"'{TOKENS_FOLDER_ID}' in parents and name = '{file_name}'"
        response = drive_service.files().list(
            q=query,
            fields='files(id)'
        ).execute()
        
        files = response.get('files', [])
        
        media = MediaIoBaseUpload(
            io.BytesIO(token_content.encode('utf-8')),
            mimetype='application/json'
        )
        
        if files:
            # Update existing file
            file_id = files[0]['id']
            drive_service.files().update(
                fileId=file_id,
                media_body=media
            ).execute()
        else:
            # Create new file
            file_metadata = {
                'name': file_name,
                'parents': [TOKENS_FOLDER_ID]
            }
            drive_service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
        
        return True
    except Exception as e:
        logger.error(f"Error saving user token to Drive: {e}")
        return False

async def delete_user_token_from_drive(user_id: int):
    """Delete user token from Google Drive folder."""
    if not drive_service:
        return False
    
    try:
        # Search for user token file in the tokens folder
        query = f"'{TOKENS_FOLDER_ID}' in parents and name = 'token_{user_id}.json'"
        response = drive_service.files().list(
            q=query,
            fields='files(id)'
        ).execute()
        
        files = response.get('files', [])
        if files:
            drive_service.files().delete(fileId=files[0]['id']).execute()
            return True
    except Exception as e:
        logger.error(f"Error deleting user token from Drive: {e}")
    
    return False

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send a welcome message with inline keyboard."""
    if not await check_channel_membership(update, context):
        return
    
    query = update.callback_query
    user_id = update.message.from_user.id if update.message else query.from_user.id
    current_plan = is_subscribed_user(user_id)
    
    plan_status = ""
    if current_plan != 'free':
        plan_status = f"\n\n✨ Your Current Plan: {current_plan.capitalize()}"
    
    keyboard = [
        [InlineKeyboardButton("🔑 Connect Google Drive", callback_data='start_auth')],
        [InlineKeyboardButton("📊 View Plans", callback_data='show_plans')],
        [InlineKeyboardButton("🛠 Help", callback_data='help')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    welcome_msg = (
        "🚀 *Welcome to the Google Drive Manager Bot!*\n\n"
        "I can help you copy Google Drive folders to your account."
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
    """Show available plans with inline buttons"""
    if not await check_channel_membership(update, context):
        return
    
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    current_plan = is_subscribed_user(user_id)
    
    plan_status = ""
    if current_plan != 'free':
        plan_status = f"\n\n✨ Your Current Plan: {current_plan.capitalize()}"
    
    keyboard = [
        [InlineKeyboardButton("🆓 Free Plan", callback_data='plan_free')],
        [InlineKeyboardButton("⭐ Basic Plan", callback_data='plan_basic')],
        [InlineKeyboardButton("💎 Premium Plan", callback_data='plan_premium')],
        [InlineKeyboardButton("💳 Payment Methods", callback_data='payment_methods')],
        [InlineKeyboardButton("🔙 Back", callback_data='main_menu')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        f"📊 *Available Plans*{plan_status}\n\n"
        "Choose a plan to view details:",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def show_payment_methods(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show available payment methods"""
    if not await check_channel_membership(update, context):
        return
    
    query = update.callback_query
    await query.answer()
    
    payment_text = "💳 *Available Payment Methods*\n\n"
    for method in PAYMENT_METHODS:
        payment_text += f"• {method}\n"
    payment_text += f"\n{CONTACT_TEXT}"
    
    keyboard = [
        [InlineKeyboardButton("🔙 Back to Plans", callback_data='show_plans')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        payment_text,
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle inline button presses"""
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
    else:
        try:
            await query.message.delete()
        except:
            pass

async def plan_details(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show details for selected plan"""
    query = update.callback_query
    await query.answer()
    plan = query.data.split('_')[1]
    
    emoji = {
        'free': '🆓',
        'basic': '⭐',
        'premium': '💎'
    }.get(plan, '📊')
    
    details = [
        f"{emoji} *{plan.capitalize()} Plan Details*\n\n",
        f"• Daily Folders: {PLAN_LIMITS[plan]['daily']}\n",
        f"• Max Folder Size: {format_size(PLAN_LIMITS[plan]['size'])}\n",
        f"• Max Files per Folder: {PLAN_LIMITS[plan]['files']}\n",
        f"• Duration: {PLAN_LIMITS[plan]['duration']}\n\n"
    ]
    
    if plan != 'free':
        details.extend([
            f"💵 *Pricing*\n",
            f"• PKR {PRICING[plan]['PKR']}\n",
            f"• USD {PRICING[plan]['USD']}\n\n"
        ])
    
    details.append(CONTACT_TEXT)
    
    keyboard = [
        [InlineKeyboardButton("🔙 Back to Plans", callback_data='show_plans')],
        [InlineKeyboardButton("🔼 Upgrade Now", url=WHATSAPP_LINK)]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await query.edit_message_text(
        ''.join(details),
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show help message with available commands."""
    if not await check_channel_membership(update, context):
        return
    
    user = update.message.from_user if update.message else update.callback_query.from_user
    is_admin = user.id == ADMIN_USER_ID
    
    help_text = [
        "🛠 *Available Commands:*\n\n",
        "• /start - Start the bot\n",
        "• /delete - Remove stored authorization\n"
    ]
    
    if is_admin:
        help_text.extend([
            "\n👑 *Admin Commands:*\n",
            "• /add [user_id] [basic|premium] - Add user to subscription tier\n",
            "• /remove [user_id|all] - Remove user or all users\n"
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
        await update.callback_query.edit_message_text(
            ''.join(help_text),
            parse_mode='Markdown',
            reply_markup=reply_markup
        )

async def delete_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Delete user's authorization data with confirmation."""
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
        "⚠️ *Confirm Authorization Removal*",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def delete_confirmed(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle confirmed deletion."""
    query = update.callback_query
    user_id = query.from_user.id
    
    try:
        await query.message.delete()
    except Exception as e:
        logger.warning(f"Could not delete confirmation message: {e}")
    
    if await delete_user_token_from_drive(user_id):
        response = "✅ *Authorization Removed*\n\nYour Google Drive access has been revoked."
    else:
        response = "ℹ️ *No active authorization found.*"
    
    keyboard = [
        [InlineKeyboardButton("🔙 Back to Menu", callback_data='main_menu')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await context.bot.send_message(
        chat_id=user_id,
        text=response,
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def cancel_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle cancellation of delete operation."""
    query = update.callback_query
    await query.answer()
    try:
        await query.message.delete()
    except Exception as e:
        logger.warning(f"Could not delete message: {e}")
    
    await context.bot.send_message(
        chat_id=query.from_user.id,
        text="✅ Deletion cancelled",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 Back to Menu", callback_data='main_menu')]
        ])
    )

async def start_auth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Start authorization process with cancel button."""
    if not await check_channel_membership(update, context):
        return
    
    query = update.callback_query
    user_id = query.from_user.id
    
    # Check if user already has token in Drive
    existing_creds = await get_user_token_from_drive(user_id)
    if existing_creds and existing_creds.valid:
        await query.edit_message_text(
            "🔒 *Already Authorized*\n\n"
            "You've already granted Drive access.\n"
            "Use /delete to remove existing authorization.",
            parse_mode='Markdown'
        )
        return ConversationHandler.END

    keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data='cancel_auth')]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    flow = Flow.from_client_secrets_file(
        CLIENT_SECRET_FILE,
        scopes=SCOPES,
        redirect_uri='http://localhost:8080'
    )
    auth_url, _ = flow.authorization_url(prompt='consent')
    pending_authorizations[user_id] = flow
    
    auth_msg = (
        "🔑 *Authorization Required*\n\n"
        "1. Click the link below to authorize:\n"
        f"[Authorize Google Drive]({auth_url})\n\n"
        "2. After approving, you'll see an error page (This is normal).\n"
        "3. Just send me the complete URL from your browser's address bar.\n\n"
        "⚠️ *Note:* You may see an 'unverified app' warning. Click 'Advanced' then 'Continue'."
    )
    
    await query.edit_message_text(
        auth_msg,
        parse_mode='Markdown',
        disable_web_page_preview=True,
        reply_markup=reply_markup
    )
    return 'WAITING_FOR_CODE'

async def handle_auth_code(update: Update, context: ContextTypes.DEFAULT_TYPE, code: str):
    """Process authorization code from redirect URL"""
    user_id = update.message.from_user.id
    
    try:
        try:
            if user_id in pending_authorizations:
                flow = pending_authorizations[user_id]
                await update.message.delete()
        except:
            pass
            
        flow = pending_authorizations[user_id]
        flow.fetch_token(code=code)
        creds = flow.credentials
        
        # Save token to Drive
        if await save_user_token_to_drive(user_id, creds):
            del pending_authorizations[user_id]
            
            keyboard = [
                [InlineKeyboardButton("📊 View Plans", callback_data='show_plans')],
                [InlineKeyboardButton("🛠 Help", callback_data='help')]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await context.bot.send_message(
                chat_id=user_id,
                text="✅ *Authorization Successful!*\n\n"
                     "You can now copy Drive folders to your account.\n"
                     "Simply send me a folder link to get started!",
                parse_mode='Markdown',
                reply_markup=reply_markup
            )
        else:
            raise Exception("Failed to save authorization to Drive")
            
    except Exception as e:
        if user_id in pending_authorizations:
            del pending_authorizations[user_id]
            
        await context.bot.send_message(
            chat_id=user_id,
            text="❌ *Authorization Failed*\n\n"
                 f"Error: `{str(e)}`\n\n"
                 "Please try again using the Connect Google Drive button.",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔑 Connect Google Drive", callback_data='start_auth')]
            ])
        )
    
    return ConversationHandler.END

async def cancel_auth(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Cancel the authorization process."""
    query = update.callback_query
    user_id = query.from_user.id
    if user_id in pending_authorizations:
        del pending_authorizations[user_id]
    
    try:
        await query.message.delete()
    except Exception as e:
        logger.warning(f"Could not delete auth message: {e}")
    
    keyboard = [
        [InlineKeyboardButton("🔑 Connect Google Drive", callback_data='start_auth')],
        [InlineKeyboardButton("🔙 Back to Menu", callback_data='main_menu')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await context.bot.send_message(
        chat_id=user_id,
        text="❌ *Authorization Cancelled*\n\n"
             "You can start the authorization process again using the button below.",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )
    return ConversationHandler.END

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle all incoming messages"""
    if not await check_channel_membership(update, context):
        return
    
    user_id = update.message.from_user.id
    text = update.message.text.strip()
    
    auth_code = extract_auth_code(text)
    if auth_code:
        if user_id in pending_authorizations:
            return await handle_auth_code(update, context, auth_code)
    
    if 'drive.google.com' in text:
        return await handle_drive_link(update, context)
    
    keyboard = [
        [InlineKeyboardButton("🔑 Connect Google Drive", callback_data='start_auth')],
        [InlineKeyboardButton("📊 View Plans", callback_data='show_plans')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await update.message.reply_text(
        "Please send a Google Drive folder link or use the buttons below:",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def handle_drive_link(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle Google Drive folder links."""
    if not await check_channel_membership(update, context):
        return
    
    user = update.message.from_user
    user_id = user.id
    user_tier = is_subscribed_user(user_id)
    
    if user_id != ADMIN_USER_ID and drive_service:
        username = user.username or "no_username"
        first_name = user.first_name or "No Name"
        save_activity_log(user_id, username, first_name, update.message.text)
    
    today = datetime.now().date()
    if user_usage[user_id]['last_used'] != today:
        user_usage[user_id] = {'count': 0, 'last_used': today}
    
    if user_usage[user_id]['count'] >= PLAN_LIMITS[user_tier]['daily']:
        keyboard = [
            [InlineKeyboardButton("📊 View Plans", callback_data='show_plans')],
            [InlineKeyboardButton("🔼 Upgrade Now", url=WHATSAPP_LINK)]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            f"❌ *Daily Limit Reached*\n\n"
            f"You have reached your daily limit of {PLAN_LIMITS[user_tier]['daily']} folders.\n\n"
            f"{CONTACT_TEXT}",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
        return
    
    creds = await get_user_token_from_drive(user_id)
    if not creds:
        keyboard = [
            [InlineKeyboardButton("🔑 Connect Google Drive", callback_data='start_auth')],
            [InlineKeyboardButton("📊 View Plans", callback_data='show_plans')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "❌ *Authorization Required*\n\n"
            "Please authorize your Google account using the button below.",
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
        return
    
    try:
        link = update.message.text.strip()
        folder_id = re.search(r'/folders/([a-zA-Z0-9_-]+)', link).group(1)
        
        service = build('drive', 'v3', credentials=creds)
        progress_msg = await update.message.reply_text("⏳ *Analyzing folder...*", parse_mode='Markdown')
        
        total_files, total_size = count_files_and_size(service, folder_id)
        
        if total_size > PLAN_LIMITS[user_tier]['size']:
            keyboard = [
                [InlineKeyboardButton("📊 View Plans", callback_data='show_plans')],
                [InlineKeyboardButton("🔼 Upgrade Now", url=WHATSAPP_LINK)]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await progress_msg.edit_text(
                f"❌ *Folder Size Exceeds Limit*\n\n"
                f"This folder is `{format_size(total_size)}` (your limit: {format_size(PLAN_LIMITS[user_tier]['size'])}).\n\n"
                f"{CONTACT_TEXT}",
                parse_mode='Markdown',
                reply_markup=reply_markup
            )
            return
        
        if total_files > PLAN_LIMITS[user_tier]['files']:
            keyboard = [
                [InlineKeyboardButton("📊 View Plans", callback_data='show_plans')],
                [InlineKeyboardButton("🔼 Upgrade Now", url=WHATSAPP_LINK)]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await progress_msg.edit_text(
                f"❌ *Folder Contains Too Many Files*\n\n"
                f"This folder contains `{total_files}` files (your limit: {PLAN_LIMITS[user_tier]['files']}).\n\n"
                f"{CONTACT_TEXT}",
                parse_mode='Markdown',
                reply_markup=reply_markup
            )
            return
        
        user_usage[user_id]['count'] += 1
        
        progress_data[user_id] = {
            'total_files': total_files,
            'processed_files': 0,
            'file_types': defaultdict(int),
            'total_size': total_size,
            'message_id': progress_msg.message_id,
            'chat_id': update.message.chat_id
        }
        
        await progress_msg.edit_text(
            "🚀 *Starting Copy Process...*",
            parse_mode='Markdown'
        )
        
        context.job_queue.run_once(
            lambda ctx: copy_folder_process(ctx, user_id, folder_id),
            0,
            data={'chat_id': update.message.chat_id, 'user_id': user_id}
        )
        
    except Exception as e:
        await update.message.reply_text(
            "❌ *Error*\n\n"
            f"An error occurred: `{str(e)}`\n\n"
            "Please try again or contact support.",
            parse_mode='Markdown'
        )

async def update_progress(context: ContextTypes.DEFAULT_TYPE, user_id: int, message: str):
    """Update the progress message in Telegram."""
    try:
        if user_id not in progress_data:
            return
            
        chat_id = progress_data[user_id].get('chat_id')
        message_id = progress_data[user_id].get('message_id')
        
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
            logger.warning(f"Failed to edit progress message: {e}")
            msg = await context.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode='Markdown'
            )
            progress_data[user_id]['message_id'] = msg.message_id
    except Exception as e:
        logger.error(f"Error updating progress: {e}")

async def copy_folder_process(context: ContextTypes.DEFAULT_TYPE, user_id: int, folder_id: str):
    """Copy a Google Drive folder and update progress."""
    job = context.job
    chat_id = job.data['chat_id']
    
    try:
        creds = await get_user_token_from_drive(user_id)
        service = build('drive', 'v3', credentials=creds)
        
        await update_progress(context, user_id, "🔍 *Analyzing folder contents...*")
        total_files, total_size = count_files_and_size(service, folder_id)
        
        progress_data[user_id].update({
            'total_files': total_files,
            'total_size': total_size
        })
        
        await update_progress(context, user_id, "🚀 *Copying files...*")
        await copy_folder(service, folder_id, None, user_id, context)
        
        success_msg = (
            "✅ *Copy Complete!*\n\n"
            f"📂 *Total Files:* `{total_files}`\n"
            f"📦 *Total Size:* `{format_size(total_size)}`\n"
            f"📊 *File Types:*\n{format_file_types(progress_data[user_id]['file_types'])}"
        )
        
        keyboard = [
            [InlineKeyboardButton("📊 View Plans", callback_data='show_plans')],
            [InlineKeyboardButton("🔙 Back to Menu", callback_data='main_menu')]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await context.bot.send_message(
            chat_id=chat_id,
            text=success_msg,
            parse_mode='Markdown',
            reply_markup=reply_markup
        )
        
    except Exception as e:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"❌ *Error*\n\n`{str(e)}`",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🛠 Help", callback_data='help')]
            ])
        )
    finally:
        if user_id in progress_data:
            try:
                if progress_data[user_id]['message_id'] is not None:
                    await context.bot.delete_message(
                        chat_id=chat_id,
                        message_id=progress_data[user_id]['message_id']
                    )
            except Exception as e:
                logger.warning(f"Could not delete progress message: {e}")
            del progress_data[user_id]

def count_files_and_size(service, folder_id: str) -> tuple:
    """Count the number of files and total size in a folder."""
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
    """Copy a folder and its contents."""
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
                progress_data[user_id]['file_types'][categorize_file(file['mimeType'])] += 1
                
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
        logger.error(f'Drive API Error: {e}')
        raise

async def update_progress_ui(context: ContextTypes.DEFAULT_TYPE, user_id: int):
    """Update the progress UI in Telegram."""
    data = progress_data.get(user_id, {})
    if not data:
        return
    
    progress = (data['processed_files'] / data['total_files']) * 100 if data['total_files'] > 0 else 0
    message = (
        f"📁 *Progress:* `{progress:.1f}%`\n"
        f"📦 *Size:* `{format_size(data['total_size'])}`\n"
        f"📊 *File Types:*\n{format_file_types(data['file_types'])}"
    )
    
    try:
        await update_progress(
            context,
            user_id,
            message
        )
    except Exception as e:
        logger.error(f"Progress update error: {e}")

def format_size(size_bytes: int) -> str:
    """Convert bytes to a human-readable format."""
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
    """Categorize a file based on its MIME type."""
    for pattern, category in FILE_TYPES.items():
        if mime_type.startswith(pattern):
            return category
    return 'Other'

def extract_auth_code(url: str) -> str:
    """Extract authorization code from redirect URL"""
    parsed = urlparse(url)
    if parsed.netloc in ['localhost', 'localhost:8080']:
        query = parse_qs(parsed.query)
        return query.get('code', [None])[0]
    return None

def is_subscribed_user(user_id: int) -> str:
    """Check user's subscription tier."""
    if user_id in PREMIUM_USERS:
        return 'premium'
    if user_id in BASIC_USERS:
        return 'basic'
    return 'free'

async def add_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /add command (admin only)."""
    user = update.message.from_user
    
    if user.id != ADMIN_USER_ID:
        await update.message.reply_text(
            "❌ *Permission Denied*\nThis command is for admins only.",
            parse_mode='Markdown'
        )
        return
    
    args = context.args
    if len(args) != 2:
        await update.message.reply_text(
            "❌ *Invalid Format*\nUsage: `/add [user_id] [basic|premium]`",
            parse_mode='Markdown'
        )
        return
    
    try:
        user_id = int(args[0])
        tier = args[1].lower()
        if tier not in ['basic', 'premium']:
            raise ValueError
    except:
        await update.message.reply_text(
            "❌ *Invalid Arguments*\nUsage: `/add [user_id] [basic|premium]`",
            parse_mode='Markdown'
        )
        return
    
    global PREMIUM_USERS, BASIC_USERS
    if tier == 'premium':
        BASIC_USERS.discard(user_id)
        PREMIUM_USERS.add(user_id)
    else:
        PREMIUM_USERS.discard(user_id)
        BASIC_USERS.add(user_id)
    
    if save_subscribed_users():
        await update.message.reply_text(
            f"✅ *User {user_id} added to {tier} tier*",
            parse_mode='Markdown'
        )
    else:
        await update.message.reply_text(
            "❌ *Failed to update user list*",
            parse_mode='Markdown'
        )

async def remove_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /remove command (admin only)."""
    user = update.message.from_user
    
    if user.id != ADMIN_USER_ID:
        await update.message.reply_text(
            "❌ *Permission Denied*\nThis command is for admins only.",
            parse_mode='Markdown'
        )
        return

    args = context.args
    if not args:
        await update.message.reply_text(
            "❌ *Invalid Format*\nUsage: `/remove [user_id|all]`",
            parse_mode='Markdown'
        )
        return

    target = args[0].lower()
    
    global PREMIUM_USERS, BASIC_USERS
    
    try:
        if target == 'all':
            PREMIUM_USERS.clear()
            BASIC_USERS.clear()
            success_msg = "✅ *Removed all users from subscription lists*"
        else:
            user_id = int(target)
            PREMIUM_USERS.discard(user_id)
            BASIC_USERS.discard(user_id)
            success_msg = f"✅ *Removed user {user_id} from subscription lists*"

        if save_subscribed_users():
            await update.message.reply_text(success_msg, parse_mode='Markdown')
        else:
            await update.message.reply_text("❌ *Failed to update user list*", parse_mode='Markdown')

    except ValueError:
        await update.message.reply_text(
            "❌ *Invalid User ID*\nPlease provide a numeric user ID or 'all'",
            parse_mode='Markdown'
        )

async def reload_users(context: ContextTypes.DEFAULT_TYPE):
    """Periodically reload user lists."""
    global PREMIUM_USERS, BASIC_USERS
    load_subscribed_users()
    logger.info("Reloaded user lists from Drive")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle errors."""
    logger.error(f"Update {update} caused error {context.error}", exc_info=context.error)
    
    if isinstance(context.error, telegram.error.BadRequest):
        if "Message to edit not found" in str(context.error):
            return
        elif "Message is not modified" in str(context.error):
            return
    
    try:
        await context.bot.send_message(
            chat_id=ADMIN_USER_ID,
            text=f"Error occurred: {context.error}\n\nUpdate: {update}"
        )
    except:
        pass

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
                text="⚠️ *Please join our channel first*\n\n"
                     f"You need to join {REQUIRED_CHANNEL} to use this bot.",
                parse_mode='Markdown',
                reply_markup=reply_markup
            )
            return False
        return True
    except Exception as e:
        logger.error(f"Error checking channel membership: {e}")
        return True

async def shutdown(signal, loop):
    """Cleanup tasks tied to the service's shutdown."""
    logger.info(f"Received exit signal {signal.name}...")
    
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    
    logger.info(f"Cancelling {len(tasks)} outstanding tasks")
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

async def run_bot():
    """Run the Telegram bot with proper initialization"""
    application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
    
    # Conversation handler for user authorization flow
    auth_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(start_auth, pattern='^start_auth$')],
        states={
            'WAITING_FOR_CODE': [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_auth_code)
            ],
        },
        fallbacks=[
            CallbackQueryHandler(cancel_auth, pattern='^cancel_auth$')
        ],
        per_message=False
    )

    # Command handlers
    command_handlers = [
        CommandHandler('start', start),
        CommandHandler('help', help_command),
        CommandHandler('delete', delete_command),
        CommandHandler('add', add_user_command),
        CommandHandler('remove', remove_user_command)
    ]

    # Add all handlers to application
    for handler in command_handlers:
        application.add_handler(handler)

    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(auth_conv)

    application.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND, 
        handle_message
    ))

    application.add_error_handler(error_handler)

    application.job_queue.run_repeating(
        reload_users,
        interval=300,
        first=10
    )

    logger.info("Starting bot components...")
    await application.initialize()
    await application.start()
    await application.updater.start_polling()
    
    while True:
        await asyncio.sleep(3600)

async def main():
    """Main entry point with web server and bot"""
    initialize_drive_service()
    
    webserver_task = asyncio.create_task(run_webserver())
    ping_task = asyncio.create_task(self_ping())
    bot_task = asyncio.create_task(run_bot())
    
    try:
        await asyncio.gather(webserver_task, ping_task, bot_task)
    except asyncio.CancelledError:
        logger.info("Shutting down gracefully...")
    except Exception as e:
        logger.error(f"Fatal error: {str(e)}")
    finally:
        logger.info("Starting cleanup process...")
        
        ping_task.cancel()
        try:
            await ping_task
        except asyncio.CancelledError:
            pass
            
        global runner, site
        if site:
            await site.stop()
        if runner:
            await runner.cleanup()
            
        application = Application.builder().token(TELEGRAM_BOT_TOKEN).build()
        await application.stop()
        await application.shutdown()
        
        logger.info("Cleanup completed")

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
    for s in signals:
        loop.add_signal_handler(
            s, lambda s=s: asyncio.create_task(shutdown(s, loop))
        )
    
    try:
        logger.info("Starting service...")
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
    except Exception as e:
        logger.error(f"Critical failure: {str(e)}")
    finally:
        loop.close()
        logger.info("Event loop closed")
