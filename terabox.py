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

# Environment variables
API_ID = os.environ.get('TELEGRAM_API', '')
if not API_ID:
    logger.error("TELEGRAM_API variable is missing! Exiting now")
    exit(1)

API_HASH = os.environ.get('TELEGRAM_HASH', '')
if not API_HASH:
    logger.error("TELEGRAM_HASH variable is missing! Exiting now")
    exit(1)
    
BOT_TOKEN = os.environ.get('BOT_TOKEN', '')
if not BOT_TOKEN:
    logger.error("BOT_TOKEN variable is missing! Exiting now")
    exit(1)

DUMP_CHAT_ID = os.environ.get('DUMP_CHAT_ID', '')
if not DUMP_CHAT_ID:
    logger.error("DUMP_CHAT_ID variable is missing! Exiting now")
    exit(1)
try:
    DUMP_CHAT_ID = int(DUMP_CHAT_ID.strip('{}'))  # Remove curly braces if present
except ValueError:
    logger.error(f"Invalid DUMP_CHAT_ID value: {DUMP_CHAT_ID}. Must be a numeric value.")
    exit(1)
    
USER_SESSION_STRING = os.environ.get('USER_SESSION_STRING', '')
if not USER_SESSION_STRING:
    logger.info("USER_SESSION_STRING variable is missing! Bot will split Files in 2Gb...")
    USER_SESSION_STRING = None

DB_URL = os.environ.get('DB_URL', '')
if not DB_URL:
    logger.error("DB_URL variable is missing! Exiting now")
    exit(1)

DB_NAME = os.environ.get('DB_NAME', 'JetMirrorBot')

# For FSUB_ID
FSUB_ID = os.environ.get('FSUB_ID', '')
if not FSUB_ID:
    logger.error("FSUB_ID variable is missing! Exiting now")
    exit(1)
try:
    FSUB_ID = int(FSUB_ID.strip('{}'))
except ValueError:
    logger.error(f"Invalid FSUB_ID value: {FSUB_ID}. Must be a numeric value.")
    exit(1)

# For ADMIN
ADMIN = os.environ.get('ADMIN', '')
if not ADMIN:
    logger.error("ADMIN variable is missing! Exiting now")
    exit(1)
try:
    ADMIN = int(ADMIN.strip('{}'))
except ValueError:
    logger.error(f"Invalid ADMIN value: {ADMIN}. Must be a numeric value.")
    exit(1)

# For LOG_CHANNEL
LOG_CHANNEL = os.environ.get('LOG_CHANNEL', '')
if not LOG_CHANNEL:
    logger.error("LOG_CHANNEL variable is missing! Exiting now")
    exit(1)
try:
    LOG_CHANNEL = int(LOG_CHANNEL.strip('{}'))
except ValueError:
    logger.error(f"Invalid LOG_CHANNEL value: {LOG_CHANNEL}. Must be a numeric value.")
    exit(1)

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
last_update_time = 0

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

    async def get_all_users(self):
        all_users = self.col.find({})
        return all_users

    async def delete_user(self, user_id):
        await self.col.delete_many({'_id': int(user_id)})
    
    async def send_user_log(self, b, u):
        await b.send_message(
            LOG_CHANNEL,
            f"**--New User Started The Bot--**\n\nUser: {u.mention}\nID: `{u.id}`\nUsername: @{u.username}"
        )
        
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

    async def get_all_chats(self):
        all_chats = self.chat.find({})
        return all_chats

    async def delete_chat(self, chat_id):
        await self.chat.delete_many({'_id': int(chat_id)})
    
    async def send_chat_log(self, b, m):
        await b.send_message(
            LOG_CHANNEL,
            f"**--New Chat Started The Bot--**\n\nChat: {m.chat.title}\nID: `{m.chat.id}`\nUsername: @{m.chat.username}"
        )

# Initialize database
db = Database(DB_URL, DB_NAME)

# Helper functions
async def is_user_member(client, user_id):
    try:
        # First ensure bot is in the channel
        try:
            await client.get_chat(FSUB_ID)
        except Exception:
            logger.error(f"Bot is not in channel {FSUB_ID}")
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
    parsed_url = urlparse(url)
    return any(parsed_url.netloc.endswith(domain) for domain in VALID_DOMAINS)

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
    await db.add_user(client, message)
    
    join_button = InlineKeyboardButton("ᴊᴏɪɴ", url="https://t.me/tellymirror")
    developer_button = InlineKeyboardButton("ᴅᴇᴠᴇʟᴏᴘᴇʀ", url="https://t.me/tellyhubownerbot")
    user_mention = message.from_user.mention
    reply_markup = InlineKeyboardMarkup([[join_button, developer_button]])
    final_msg = f"ᴡᴇʟᴄᴏᴍᴇ, {user_mention}.\n\nɪ ᴀᴍ ᴀ ᴛᴇʀᴀʙᴏx ᴅᴏᴡɴʟᴏᴀᴅᴇʀ ʙᴏᴛ. sᴇɴᴅ ᴍᴇ ᴀɴʏ ᴛᴇʀᴀʙᴏx ʟɪɴᴋ ɪ ᴡɪʟʟ ᴅᴏᴡɴʟᴏᴀᴅ ᴡɪᴛʜɪɴ ғᴇᴡ sᴇᴄᴏɴᴅs ᴀɴᴅ sᴇɴᴅ ɪᴛ ᴛᴏ ʏᴏᴜ ✨."
    video_file_id = "https://ibb.co/m5yY6gDs"
    if os.path.exists(video_file_id):
        await client.send_video(
            chat_id=message.chat.id,
            video=video_file_id,
            caption=final_msg,
            reply_markup=reply_markup
        )
    else:
        await message.reply_text(final_msg, reply_markup=reply_markup)


@app.on_message(filters.private & filters.command("restart") & filters.user(ADMIN))
async def restart_bot(b, m):
    sts = await b.send_message(text="**🔄 Processes stopped. Bot is restarting.....**", chat_id=m.chat.id)
    failed = 0
    success = 0
    deactivated = 0
    blocked = 0
    start_time = time.time()
    total_users = await db.total_users_count()
    all_users = await db.get_all_users()
    async for user in all_users:
        try:
            restart_msg = f"Hey, {(await b.get_users(user['_id'])).mention}\n\n**🔄 Processes stopped. Bot is restarting.....\n\n✅️ Bot is restarted. Now you can use me.**"
            await b.send_message(user['_id'], restart_msg)
            success += 1
        except InputUserDeactivated:
            deactivated += 1
            await db.delete_user(user['_id'])
        except UserIsBlocked:
            blocked += 1
            await db.delete_user(user['_id'])
        except Exception as e:
            failed += 1
            await db.delete_user(user['_id'])
            logger.error(f"Error sending restart message: {e}")
        try:
            await sts.edit(f"<u>Restart in progress:</u>\n\n• Total users: {total_users}\n• Successful: {success}\n• Blocked users: {blocked}\n• Deleted accounts: {deactivated}\n• Unsuccessful: {failed}")
        except FloodWait as e:
            await asyncio.sleep(e.value)
    completed_restart = timedelta(seconds=int(time.time() - start_time))
    await sts.edit(f"Completed restart: {completed_restart}\n\n• Total users: {total_users}\n• Successful: {success}\n• Blocked users: {blocked}\n• Deleted accounts: {deactivated}\n• Unsuccessful: {failed}")
    os.execl(sys.executable, sys.executable, *sys.argv)


# Add this with your other global variables
BOT_START_TIME = time.time()

# Then modify your stats command:
@app.on_message(filters.command(["stats", "status"]) & filters.user(ADMIN))
async def get_stats(bot: Client, message: Message):
    try:
        total_users = await db.total_users_count()
        total_chats = await db.total_chats_count()
        uptime_seconds = time.time() - BOT_START_TIME
        uptime = str(timedelta(seconds=uptime_seconds)).split(".")[0]  # Format as HH:MM:SS
        
        start_t = time.time()
        sts = await message.reply('**Processing.....**')    
        end_t = time.time()
        time_taken_s = (end_t - start_t) * 1000
        
        await sts.edit(
            text=f"**--Bot Status--**\n\n"
                 f"**⌚️ Bot Uptime:** {uptime}\n"
                 f"**🐌 Current Ping:** `{time_taken_s:.3f} ms`\n"
                 f"**👭 Total Users:** `{total_users}`\n"
                 f"**💬 Total Chats:** `{total_chats}`"
        )
    except Exception as e:
        logger.error(f"Error in stats command: {e}")
        await message.reply("❌ Error getting stats. Check logs.")

@app.on_message(filters.command("broadcast") & filters.user(ADMIN) & filters.reply)
async def broadcast_handler(bot: Client, message: Message):
    try:
        await bot.send_message(
            LOG_CHANNEL,
            f"{message.from_user.mention} ({message.from_user.id}) started a broadcast"
        )
        
        broadcast_msg = message.reply_to_message
        if not broadcast_msg:
            await message.reply("Please reply to a message to broadcast")
            return
            
        sts_msg = await message.reply("Broadcast Started..!") 
        done = 0
        failed = 0
        success = 0
        start_time = time.time()
        
        total_users = await db.total_users_count()
        all_users = db.get_all_users()
        
        async for user in all_users:
            try:
                sts = await send_msg(user['_id'], broadcast_msg)
                if sts == 200:
                    success += 1
                else:
                    failed += 1
                done += 1
                
                if done % 20 == 0:
                    await sts_msg.edit(
                        f"Broadcast Progress:\n"
                        f"Total Users: {total_users}\n"
                        f"Completed: {done}/{total_users}\n"
                        f"Success: {success}\n"
                        f"Failed: {failed}"
                    )
            except Exception as e:
                logger.error(f"Error broadcasting to {user['_id']}: {e}")
                failed += 1
                continue
                
        completed_in = timedelta(seconds=int(time.time() - start_time))
        await sts_msg.edit(
            f"Broadcast Completed in {completed_in}\n\n"
            f"Total Users: {total_users}\n"
            f"Success: {success}\n"
            f"Failed: {failed}"
        )
    except Exception as e:
        logger.error(f"Broadcast error: {e}")
        await message.reply(f"❌ Broadcast failed: {e}")



@app.on_message(filters.text)
async def handle_message(client: Client, message: Message):
    if message.text.startswith('/'):
        return
    if not message.from_user:
        return

    user_id = message.from_user.id
    is_member = await is_user_member(client, user_id)

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
        await message.reply_text("Please provide a valid Terabox link.")
        return

    encoded_url = urllib.parse.quote(url)
    final_url = f"https://teraboxbotredirect.tellycloudapi.workers.dev/?url={encoded_url}"

    download = aria2.add_uris([final_url])
    status_message = await message.reply_text("sᴇɴᴅɪɴɢ ʏᴏᴜ ᴛʜᴇ ᴍᴇᴅɪᴀ...🤤")

    start_time = datetime.now()

    while not download.is_complete:
        await asyncio.sleep(15)
        download.update()
        progress = download.progress

        elapsed_time = datetime.now() - start_time
        elapsed_minutes, elapsed_seconds = divmod(elapsed_time.seconds, 60)

        status_text = (
            f"ғɪʟᴇɴᴀᴍᴇ: {download.name}\n"
            f"[{'▣' * int(progress / 10)}{'▢' * (10 - int(progress / 10))}] {progress:.2f}%\n"
            f"ᴘʀᴏᴄᴇssᴇᴅ: {format_size(download.completed_length)} ᴏғ {format_size(download.total_length)}\n"
            f"sᴛᴀᴛᴜs: 📥 Downloading\n"
            f"sᴘᴇᴇᴅ: {format_size(download.download_speed)}/s\n"
            f"ᴇᴛᴀ: {download.eta} | ᴇʟᴀᴘsᴇᴅ: {elapsed_minutes}m {elapsed_seconds}s\n"
        )
        while True:
            try:
                await update_status_message(status_message, status_text)
                break
            except FloodWait as e:
                logger.error(f"Flood wait detected! Sleeping for {e.value} seconds")
                await asyncio.sleep(e.value)

    file_path = download.files[0].path
    caption = f"✨ {download.name}"

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
                await update_status(message, text)
            except Exception as e:
                logger.error(f"Error updating status: {e}")

    async def upload_progress(current, total):
        progress = (current / total) * 100
        elapsed_time = datetime.now() - start_time
        elapsed_minutes, elapsed_seconds = divmod(elapsed_time.seconds, 60)

        status_text = (
            f"ғɪʟᴇɴᴀᴍᴇ: {download.name}\n"
            f"[{'▣' * int(progress / 10)}{'▢' * (10 - int(progress / 10))}] {progress:.2f}%\n"
            f"ᴘʀᴏᴄᴇssᴇᴅ: {format_size(current)} ᴏғ {format_size(total)}\n"
            f"sᴛᴀᴛᴜs: 📤 Uploading to Telegram\n"
            f"sᴘᴇᴇᴅ: {format_size(current / elapsed_time.seconds if elapsed_time.seconds > 0 else 0)}/s\n"
            f"ᴇʟᴀᴘsᴇᴅ: {elapsed_minutes}m {elapsed_seconds}s\n"
        )
        await update_status(status_message, status_text)

    async def split_video_with_ffmpeg(input_path, output_prefix, split_size):
        try:
            original_ext = os.path.splitext(input_path)[1].lower() or '.mp4'
            start_time = datetime.now()
            last_progress_update = time.time()
            
            proc = await asyncio.create_subprocess_exec(
                'ffprobe', '-v', 'error', '-show_entries', 'format=duration',
                '-of', 'default=noprint_wrappers=1:nokey=1', input_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, _ = await proc.communicate()
            total_duration = float(stdout.decode().strip())
            
            file_size = os.path.getsize(input_path)
            parts = math.ceil(file_size / split_size)
            
            if parts == 1:
                return [input_path]
            
            duration_per_part = total_duration / parts
            split_files = []
            
            for i in range(parts):
                current_time = time.time()
                if current_time - last_progress_update >= UPDATE_INTERVAL:
                    elapsed = datetime.now() - start_time
                    status_text = (
                        f"✂️ Splitting {os.path.basename(input_path)}\n"
                        f"Part {i+1}/{parts}\n"
                        f"Elapsed: {elapsed.seconds // 60}m {elapsed.seconds % 60}s"
                    )
                    await update_status(status_message, status_text)
                    last_progress_update = current_time
                
                output_path = f"{output_prefix}.{i+1:03d}{original_ext}"
                cmd = [
                    'ffmpeg', '-y', '-ss', str(i * duration_per_part),
                    '-i', input_path, '-t', str(duration_per_part),
                    '-c', 'copy', '-map', '0',
                    '-avoid_negative_ts', 'make_zero',
                    output_path
                ]
                
                proc = await asyncio.create_subprocess_exec(*cmd)
                await proc.wait()
                split_files.append(output_path)
            
            return split_files
        except Exception as e:
            logger.error(f"Split error: {e}")
            raise

    async def handle_upload():
        file_size = os.path.getsize(file_path)
        
        if file_size > SPLIT_SIZE:
            await update_status(
                status_message,
                f"✂️ Splitting {download.name} ({format_size(file_size)})"
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
                        f"📤 Uploading part {i+1}/{len(split_files)}\n"
                        f"{os.path.basename(part)}"
                    )
                    
                    if USER_SESSION_STRING:
                        sent = await user.send_video(
                            DUMP_CHAT_ID, part, 
                            caption=part_caption,
                            progress=upload_progress
                        )
                        await app.copy_message(
                            message.chat.id, DUMP_CHAT_ID, sent.id
                        )
                    else:
                        sent = await client.send_video(
                            DUMP_CHAT_ID, part,
                            caption=part_caption,
                            progress=upload_progress
                        )
                        await client.send_video(
                            message.chat.id, sent.video.file_id,
                            caption=part_caption
                        )
                    os.remove(part)
            finally:
                for part in split_files:
                    try: os.remove(part)
                    except: pass
        else:
            await update_status(
                status_message,
                f"📤 Uploading {download.name}\n"
                f"Size: {format_size(file_size)}"
            )
            
            if USER_SESSION_STRING:
                sent = await user.send_video(
                    DUMP_CHAT_ID, file_path,
                    caption=caption,
                    progress=upload_progress
                )
                await app.copy_message(
                    message.chat.id, DUMP_CHAT_ID, sent.id
                )
            else:
                sent = await client.send_video(
                    DUMP_CHAT_ID, file_path,
                    caption=caption,
                    progress=upload_progress
                )
                await client.send_video(
                    message.chat.id, sent.video.file_id,
                    caption=caption
                )
        if os.path.exists(file_path):
            os.remove(file_path)

    start_time = datetime.now()
    await handle_upload()

    try:
        await status_message.delete()
        await message.delete()
    except Exception as e:
        logger.error(f"Cleanup error: {e}")

# Flask server for keeping the bot alive
flask_app = Flask(__name__)

@flask_app.route('/')
def home():
    return render_template("index.html")

def run_flask():
    flask_app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

def keep_alive():
    Thread(target=run_flask).start()

async def start_user_client():
    if user:
        await user.start()
        logger.info("User client started.")

def run_user():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(start_user_client())

if __name__ == "__main__":
    keep_alive()

    if user:
        logger.info("Starting user client...")
        Thread(target=run_user).start()

    logger.info("Starting bot client...")
    app.run()
