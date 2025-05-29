from aria2p import API as Aria2API, Client as Aria2Client
import asyncio
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os
import logging
import math
from pyrogram import Client, filters
from pyrogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
from pyrogram.enums import ChatMemberStatus
from pyrogram.errors import FloodWait, InputUserDeactivated, UserIsBlocked, PeerIdInvalid
import time
import urllib.parse
from urllib.parse import urlparse
from flask import Flask, render_template
from threading import Thread
import motor.motor_asyncio
import sys

# Load environment variables
load_dotenv('config.env', override=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s - %(name)s - %(levelname)s] %(message)s - %(filename)s:%(lineno)d"
)
logger = logging.getLogger(__name__)
logging.getLogger("pyrogram.session").setLevel(logging.ERROR)
logging.getLogger("pyrogram.connection").setLevel(logging.ERROR)
logging.getLogger("pyrogram.dispatcher").setLevel(logging.ERROR)

# Global start time for uptime calculation
BOT_START_TIME = time.time()

# Initialize aria2
aria2 = Aria2API(
    Aria2Client(
        host="http://localhost",
        port=6800,
        secret=""
    )
)
options = {
    "max-tries": "50",
    "retry-wait": "3",
    "continue": "true",
    "allow-overwrite": "true",
    "min-split-size": "4M",
    "split": "10"
}
aria2.set_global_options(options)

# Environment variables with error handling
def get_env_int(var_name, required=True):
    value = os.environ.get(var_name, '')
    if not value and required:
        logger.error(f"{var_name} variable is missing! Exiting now")
        exit(1)
    try:
        # Remove any surrounding characters
        return int(value.strip('{}\'" '))
    except ValueError:
        logger.error(f"Invalid {var_name} value: {value}. Must be a numeric value.")
        exit(1)

def get_env_str(var_name, required=True, default=''):
    value = os.environ.get(var_name, default)
    if not value and required:
        logger.error(f"{var_name} variable is missing! Exiting now")
        exit(1)
    return value.strip('{}\'" ')

# Get environment variables
API_ID = get_env_int('TELEGRAM_API')
API_HASH = get_env_str('TELEGRAM_HASH')
BOT_TOKEN = get_env_str('BOT_TOKEN')
DUMP_CHAT_ID = get_env_int('DUMP_CHAT_ID')
FSUB_ID = get_env_int('FSUB_ID')
ADMIN = get_env_int('ADMIN')
LOG_CHANNEL = get_env_int('LOG_CHANNEL')
DB_URL = get_env_str('DB_URL')
DB_NAME = get_env_str('DB_NAME', required=False, default='JetMirrorBot')
USER_SESSION_STRING = get_env_str('USER_SESSION_STRING', required=False)

# Initialize clients
app = Client("jetbot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
user = None
SPLIT_SIZE = 2093796556  # 2GB
if USER_SESSION_STRING:
    user = Client("jetu", api_id=API_ID, api_hash=API_HASH, session_string=USER_SESSION_STRING)
    SPLIT_SIZE = 4241280205  # 4GB

VALID_DOMAINS = [
    'terabox.com', 'nephobox.com', '4funbox.com', 'mirrobox.com', 
    'momerybox.com', 'teraboxapp.com', '1024tera.com', 
    'terabox.app', 'gibibox.com', 'goaibox.com', 'terasharelink.com', 
    'teraboxlink.com', 'terafileshare.com'
]

# Database class
class Database:
    def __init__(self, uri, database_name):
        self._client = motor.motor_asyncio.AsyncIOMotorClient(uri)
        self.db = self._client[database_name]
        self.col = self.db.users
        self.chat = self.db.chats
        
    def new_user(self, id):
        return dict(_id=int(id))
            
    async def add_user(self, b, m):
        u = m.from_user
        if not await self.is_user_exist(u.id):
            user = self.new_user(u.id)
            await self.col.insert_one(user)            
            await self.send_user_log(b, u)

    async def is_user_exist(self, id):
        user = await self.col.find_one({'_id': int(id)})
        return bool(user)

    async def total_users_count(self):
        count = await self.col.count_documents({})
        return count

    def get_all_users(self):
        return self.col.find({})

    async def delete_user(self, user_id):
        await self.col.delete_many({'_id': int(user_id)})
    
    async def send_user_log(self, b, u):
        try:
            await b.send_message(
                LOG_CHANNEL,
                f"**--New User Started The Bot--**\n\nUser: {u.mention}\nID: `{u.id}`\nUsername: @{u.username}"
            )
        except Exception as e:
            logger.error(f"Failed to send user log: {e}")
        
    async def add_chat(self, b, m):
        if not await self.is_chat_exist(m.chat.id):
            chat = self.new_user(m.chat.id)
            await self.chat.insert_one(chat)            
            await self.send_chat_log(b, m)

    async def is_chat_exist(self, id):
        chat = await self.chat.find_one({'_id': int(id)})
        return bool(chat)

    async def total_chats_count(self):
        count = await self.chat.count_documents({})
        return count

    def get_all_chats(self):
        return self.chat.find({})

    async def delete_chat(self, chat_id):
        await self.chat.delete_many({'_id': int(chat_id)})
    
    async def send_chat_log(self, b, m):
        try:
            await b.send_message(
                LOG_CHANNEL,
                f"**--New Chat Started The Bot--**\n\nChat: {m.chat.title}\nID: `{m.chat.id}`\nUsername: @{m.chat.username}"
            )
        except Exception as e:
            logger.error(f"Failed to send chat log: {e}")

# Initialize database
db = Database(DB_URL, DB_NAME)

# Helper functions
async def is_user_member(client, user_id):
    try:
        # Check if bot is in the channel
        try:
            await client.get_chat_member(FSUB_ID, "me")
        except Exception:
            logger.warning(f"Bot is not in channel {FSUB_ID} or not admin")
            return True  # Skip check if bot isn't in channel
            
        member = await client.get_chat_member(FSUB_ID, user_id)
        return member.status in [
            ChatMemberStatus.MEMBER, 
            ChatMemberStatus.ADMINISTRATOR, 
            ChatMemberStatus.OWNER
        ]
    except Exception as e:
        logger.error(f"Error checking membership for {user_id}: {e}")
        return True  # Default to True if check fails
    
def is_valid_url(url):
    try:
        parsed_url = urlparse(url)
        return any(parsed_url.netloc.endswith(domain) for domain in VALID_DOMAINS)
    except Exception:
        return False

def format_size(size):
    if size < 1024:
        return f"{size} B"
    elif size < 1024 * 1024:
        return f"{size / 1024:.2f} KB"
    elif size < 1024 * 1024 * 1024:
        return f"{size / (1024 * 1024):.2f} MB"
    else:
        return f"{size / (1024 * 1024 * 1024):.2f} GB"

async def update_status_message(status_message, text):
    try:
        await status_message.edit_text(text)
    except Exception as e:
        logger.error(f"Failed to update status message: {e}")

async def send_msg(user_id, message):
    try:
        await message.copy(chat_id=int(user_id))
        return 200
    except FloodWait as e:
        await asyncio.sleep(e.value)
        return await send_msg(user_id, message)
    except InputUserDeactivated:
        logger.info(f"{user_id} : Deactivated")
        return 400
    except UserIsBlocked:
        logger.info(f"{user_id} : Blocked The Bot")
        return 400
    except PeerIdInvalid:
        logger.info(f"{user_id} : User Id Invalid")
        return 400
    except Exception as e:
        logger.error(f"{user_id} : {e}")
        return 500

# Command handlers
@app.on_message(filters.command("start"))
async def start_command(client: Client, message: Message):
    try:
        await db.add_user(client, message)
    except Exception as e:
        logger.error(f"Error adding user to DB: {e}")
    
    join_button = InlineKeyboardButton("ᴊᴏɪɴ", url="https://t.me/tellymirror")
    developer_button = InlineKeyboardButton("ᴅᴇᴠᴇʟᴏᴘᴇʀ", url="https://t.me/tellyhubownerbot")
    user_mention = message.from_user.mention
    reply_markup = InlineKeyboardMarkup([[join_button, developer_button]])
    final_msg = f"ᴡᴇʟᴄᴏᴍᴇ, {user_mention}.\n\nɪ ᴀᴍ ᴀ ᴛᴇʀᴀʙᴏx ᴅᴏᴡɴʟᴏᴀᴅᴇʀ ʙᴏᴛ. sᴇɴᴅ ᴍᴇ ᴀɴʏ ᴛᴇʀᴀʙᴏx ʟɪɴᴋ ɪ ᴡɪʟʟ ᴅᴏᴡɴʟᴏᴀᴅ ᴡɪᴛʜɪɴ ғᴇᴡ sᴇᴄᴏɴᴅs ᴀɴᴅ sᴇɴᴅ ɪᴛ ᴛᴏ ʏᴏᴜ ✨."
    await message.reply_text(final_msg, reply_markup=reply_markup)

@app.on_message(filters.command(["stats", "status"]) & filters.user(ADMIN))
async def get_stats(bot: Client, message: Message):
    try:
        total_users = await db.total_users_count()
        total_chats = await db.total_chats_count()
        uptime_seconds = time.time() - BOT_START_TIME
        uptime = str(timedelta(seconds=int(uptime_seconds)))
        
        start_t = time.time()
        sts = await message.reply('**Processing.....**')    
        end_t = time.time()
        time_taken_s = (end_t - start_t) * 1000
        
        await sts.edit(
            text=f"**--Bot Status--**\n\n"
                 f"**⌚️ Bot Uptime:** `{uptime}`\n"
                 f"**🐌 Current Ping:** `{time_taken_s:.3f} ms`\n"
                 f"**👭 Total Users:** `{total_users}`\n"
                 f"**💬 Total Chats:** `{total_chats}`"
        )
    except Exception as e:
        logger.error(f"Error in stats command: {e}")
        await message.reply("❌ Error getting stats. Check logs.")

@app.on_message(filters.private & filters.command("restart") & filters.user(ADMIN))
async def restart_bot(b, m):
    sts = await b.send_message(text="**🔄 Processes stopped. Bot is restarting.....**", chat_id=m.chat.id)
    failed = 0
    success = 0
    deactivated = 0
    blocked = 0
    start_time = time.time()
    total_users = await db.total_users_count()
    all_users = db.get_all_users()
    
    async for user_doc in all_users:
        try:
            user_id = user_doc['_id']
            try:
                user_obj = await b.get_users(user_id)
                mention = user_obj.mention
            except:
                mention = f"user:{user_id}"
                
            restart_msg = f"Hey {mention},\n\n**🔄 Processes stopped. Bot is restarting.....\n\n✅️ Bot is restarted. Now you can use me.**"
            await b.send_message(user_id, restart_msg)
            success += 1
        except InputUserDeactivated:
            deactivated += 1
            await db.delete_user(user_id)
        except UserIsBlocked:
            blocked += 1
            await db.delete_user(user_id)
        except Exception as e:
            failed += 1
            logger.error(f"Error sending restart to {user_id}: {e}")
        finally:
            if (success + failed + deactivated + blocked) % 10 == 0:
                await sts.edit(
                    f"<u>Restart in progress:</u>\n\n"
                    f"• Total users: {total_users}\n"
                    f"• Successful: {success}\n"
                    f"• Blocked users: {blocked}\n"
                    f"• Deleted accounts: {deactivated}\n"
                    f"• Unsuccessful: {failed}"
                )
    
    completed_restart = timedelta(seconds=int(time.time() - start_time))
    await sts.edit(
        f"✅ Completed restart in `{completed_restart}`\n\n"
        f"• Total users: {total_users}\n"
        f"• Successful: {success}\n"
        f"• Blocked users: {blocked}\n"
        f"• Deleted accounts: {deactivated}\n"
        f"• Unsuccessful: {failed}"
    )
    os.execl(sys.executable, sys.executable, *sys.argv)

@app.on_message(filters.command("broadcast") & filters.user(ADMIN) & filters.reply)
async def broadcast_handler(bot: Client, message: Message):
    try:
        await bot.send_message(
            LOG_CHANNEL,
            f"{message.from_user.mention} ({message.from_user.id}) started a broadcast"
        )
        
        broadcast_msg = message.reply_to_message
        if not broadcast_msg:
            await message.reply("❌ Please reply to a message to broadcast")
            return
            
        sts_msg = await message.reply("📢 Broadcast Started...") 
        done = 0
        failed = 0
        success = 0
        start_time = time.time()
        
        total_users = await db.total_users_count()
        all_users = db.get_all_users()
        
        async for user_doc in all_users:
            user_id = user_doc['_id']
            try:
                sts = await send_msg(user_id, broadcast_msg)
                if sts == 200:
                    success += 1
                else:
                    failed += 1
                done += 1
                
                if done % 20 == 0:
                    await sts_msg.edit(
                        f"📊 Broadcast Progress:\n"
                        f"• Total Users: `{total_users}`\n"
                        f"• Completed: `{done}/{total_users}`\n"
                        f"• Success: `{success}`\n"
                        f"• Failed: `{failed}`"
                    )
            except Exception as e:
                logger.error(f"Error broadcasting to {user_id}: {e}")
                failed += 1
                
        completed_in = timedelta(seconds=int(time.time() - start_time))
        await sts_msg.edit(
            f"✅ Broadcast Completed in `{completed_in}`\n\n"
            f"• Total Users: `{total_users}`\n"
            f"• Success: `{success}`\n"
            f"• Failed: `{failed}`"
        )
    except Exception as e:
        logger.error(f"Broadcast error: {e}")
        await message.reply(f"❌ Broadcast failed: {str(e)[:200]}")

@app.on_message(filters.text & ~filters.command(["start", "stats", "status", "broadcast", "restart"]))
async def handle_message(client: Client, message: Message):
    if not message.from_user:
        return

    user_id = message.from_user.id
    try:
        is_member = await is_user_member(client, user_id)
    except Exception as e:
        logger.error(f"Membership check error: {e}")
        is_member = True  # Default to True on error

    if not is_member:
        join_button = InlineKeyboardButton("ᴊᴏɪɴ ❤️🚀", url="https://t.me/tellymirror")
        reply_markup = InlineKeyboardMarkup([[join_button]])
        await message.reply_text("ʏᴏᴜ ᴍᴜsᴛ ᴊᴏɪɴ ᴍʏ ᴄʜᴀɴɴᴇʟ ᴛᴏ ᴜsᴇ ᴍᴇ.", reply_markup=reply_markup)
        return
    
    url = None
    for word in message.text.split():
        if is_valid_url(word):
            url = word
            break

    if not url:
        await message.reply_text("❌ Please provide a valid Terabox link.")
        return

    try:
        encoded_url = urllib.parse.quote(url)
        final_url = f"https://teraboxbotredirect.tellycloudapi.workers.dev/?url={encoded_url}"
    except Exception as e:
        logger.error(f"URL encoding error: {e}")
        await message.reply_text("❌ Invalid URL format")
        return

    try:
        download = aria2.add_uris([final_url])
        status_message = await message.reply_text("⏳ Processing your request...")
    except Exception as e:
        logger.error(f"Aria2 add_uris error: {e}")
        await message.reply_text("❌ Failed to start download")
        return

    start_time = datetime.now()

    while not download.is_complete:
        try:
            await asyncio.sleep(15)
            download.update()
            progress = download.progress

            elapsed_time = datetime.now() - start_time
            elapsed_minutes, elapsed_seconds = divmod(elapsed_time.seconds, 60)

            status_text = (
                f"📥 **Downloading:** `{download.name}`\n"
                f"📊 **Progress:** `{progress:.2f}%`\n"
                f"🔽 **Downloaded:** `{format_size(download.completed_length)}` / `{format_size(download.total_length)}`\n"
                f"🚀 **Speed:** `{format_size(download.download_speed)}/s`\n"
                f"⏱️ **Elapsed:** `{elapsed_minutes}m {elapsed_seconds}s`\n"
                f"⏳ **ETA:** `{download.eta}`"
            )
            await update_status_message(status_message, status_text)
        except FloodWait as e:
            logger.warning(f"Flood wait: Sleeping for {e.value}s")
            await asyncio.sleep(e.value)
        except Exception as e:
            logger.error(f"Download status error: {e}")
            break

    if not download.is_complete:
        await status_message.edit_text("❌ Download failed or timed out")
        return

    try:
        file_path = download.files[0].path
        caption = f"✨ **{download.name}**"
    except Exception as e:
        logger.error(f"File path error: {e}")
        await status_message.edit_text("❌ Error getting file path")
        return

    last_update_time = time.time()
    UPDATE_INTERVAL = 15

    async def update_status(message, text):
        nonlocal last_update_time
        current_time = time.time()
        if current_time - last_update_time >= UPDATE_INTERVAL:
            try:
                await message.edit_text(text)
                last_update_time = current_time
            except FloodWait as e:
                logger.warning(f"FloodWait: Sleeping for {e.value}s")
                await asyncio.sleep(e.value)
            except Exception as e:
                logger.error(f"Status update error: {e}")

    async def upload_progress(current, total):
        progress = (current / total) * 100
        elapsed_time = datetime.now() - start_time
        elapsed_minutes, elapsed_seconds = divmod(elapsed_time.seconds, 60)
        speed = current / elapsed_time.total_seconds() if elapsed_time.total_seconds() > 0 else 0

        status_text = (
            f"📤 **Uploading:** `{download.name}`\n"
            f"📊 **Progress:** `{progress:.2f}%`\n"
            f"🔼 **Uploaded:** `{format_size(current)}` / `{format_size(total)}`\n"
            f"🚀 **Speed:** `{format_size(speed)}/s`\n"
            f"⏱️ **Elapsed:** `{elapsed_minutes}m {elapsed_seconds}s`"
        )
        await update_status(status_message, status_text)

    async def split_video_with_ffmpeg(input_path, output_prefix, split_size):
        try:
            original_ext = os.path.splitext(input_path)[1].lower() or '.mp4'
            start_time = datetime.now()
            
            # Get video duration
            proc = await asyncio.create_subprocess_exec(
                'ffprobe', '-v', 'error', '-show_entries', 'format=duration',
                '-of', 'default=noprint_wrappers=1:nokey=1', input_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            if stderr:
                logger.error(f"FFprobe error: {stderr.decode()}")
                return [input_path]
                
            total_duration = float(stdout.decode().strip())
            
            file_size = os.path.getsize(input_path)
            parts = math.ceil(file_size / split_size)
            
            if parts == 1:
                return [input_path]
            
            duration_per_part = total_duration / parts
            split_files = []
            
            for i in range(parts):
                output_path = f"{output_prefix}.part{i+1:03d}{original_ext}"
                cmd = [
                    'ffmpeg', '-y', 
                    '-ss', str(i * duration_per_part),
                    '-i', input_path, 
                    '-t', str(duration_per_part),
                    '-c', 'copy', 
                    '-map', '0',
                    '-avoid_negative_ts', 'make_zero',
                    output_path
                ]
                
                proc = await asyncio.create_subprocess_exec(*cmd)
                await proc.wait()
                split_files.append(output_path)
                
                # Update status every part
                await update_status(
                    status_message,
                    f"✂️ **Splitting:** `{download.name}`\n"
                    f"📦 **Part:** `{i+1}/{parts}`"
                )
            
            return split_files
        except Exception as e:
            logger.error(f"Split error: {e}")
            return [input_path]

    async def handle_upload():
        try:
            file_size = os.path.getsize(file_path)
            
            if file_size > SPLIT_SIZE:
                await update_status(
                    status_message,
                    f"✂️ **Splitting:** `{download.name}`\n"
                    f"📏 **Size:** `{format_size(file_size)}`"
                )
                
                split_files = await split_video_with_ffmpeg(
                    file_path,
                    os.path.splitext(file_path)[0],
                    SPLIT_SIZE
                )
                
                try:
                    for i, part in enumerate(split_files):
                        part_caption = f"{caption}\n\nPart {i+1}/{len(split_files)}"
                        await update_status(
                            status_message,
                            f"📤 **Uploading Part:** `{i+1}/{len(split_files)}`\n"
                            f"📝 **File:** `{os.path.basename(part)}`"
                        )
                        
                        if USER_SESSION_STRING and user:
                            sent = await user.send_video(
                                DUMP_CHAT_ID, 
                                video=part, 
                                caption=part_caption,
                                progress=upload_progress
                            )
                            await app.copy_message(
                                message.chat.id, 
                                DUMP_CHAT_ID, 
                                sent.id
                            )
                        else:
                            sent = await client.send_video(
                                DUMP_CHAT_ID, 
                                video=part,
                                caption=part_caption,
                                progress=upload_progress
                            )
                            await client.send_video(
                                message.chat.id, 
                                sent.video.file_id,
                                caption=part_caption
                            )
                        try:
                            os.remove(part)
                        except:
                            pass
                finally:
                    for part in split_files:
                        try: 
                            os.remove(part)
                        except: 
                            pass
            else:
                await update_status(
                    status_message,
                    f"📤 **Uploading:** `{download.name}`\n"
                    f"📏 **Size:** `{format_size(file_size)}`"
                )
                
                if USER_SESSION_STRING and user:
                    sent = await user.send_video(
                        DUMP_CHAT_ID, 
                        video=file_path,
                        caption=caption,
                        progress=upload_progress
                    )
                    await app.copy_message(
                        message.chat.id, 
                        DUMP_CHAT_ID, 
                        sent.id
                    )
                else:
                    sent = await client.send_video(
                        DUMP_CHAT_ID, 
                        video=file_path,
                        caption=caption,
                        progress=upload_progress
                    )
                    await client.send_video(
                        message.chat.id, 
                        sent.video.file_id,
                        caption=caption
                    )
        except Exception as e:
            logger.error(f"Upload error: {e}")
            await status_message.edit_text(f"❌ Upload failed: {str(e)[:200]}")
        finally:
            try:
                os.remove(file_path)
            except:
                pass

    await handle_upload()
    try:
        await status_message.delete()
        await message.delete()
    except:
        pass

# Flask server for keeping the bot alive
flask_app = Flask(__name__)

@flask_app.route('/')
def home():
    return "Bot is running!"

def run_flask():
    flask_app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

def keep_alive():
    Thread(target=run_flask).start()

async def start_clients():
    logger.info("Starting bot client...")
    await app.start()
    
    if user:
        logger.info("Starting user client...")
        await user.start()
    
    # Set bot commands
    await app.set_bot_commands([
        ("start", "Start the bot"),
        ("stats", "Get bot statistics (admin)"),
        ("broadcast", "Broadcast message (admin)"),
        ("restart", "Restart bot (admin)")
    ])
    
    me = await app.get_me()
    logger.info(f"Bot started as @{me.username}")

async def run_bot():
    await start_clients()
    await idle()

if __name__ == "__main__":
    keep_alive()
    
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run_bot())
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(app.stop())
        if user:
            loop.run_until_complete(user.stop())
        loop.close()
