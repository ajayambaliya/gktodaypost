import requests
from bs4 import BeautifulSoup
import telegram
from telegram.constants import ParseMode
import asyncio
from deep_translator import GoogleTranslator, exceptions
import time
from pymongo import MongoClient
import re
import os

# Configuration from environment variables
MONGO_CONNECTION_STRING = os.getenv("MONGO_CONNECTION_STRING")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_USERNAME = os.getenv("TELEGRAM_CHANNEL_USERNAME")
DB_NAME = 'indiabixurl'
COLLECTION_NAME = 'ScrapedLinks'

# Initialize MongoDB client
client = MongoClient(MONGO_CONNECTION_STRING)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Function to fetch all article URLs from the given pages
def fetch_article_urls(base_url, pages):
    article_urls = []
    for page in range(1, pages + 1):
        url = base_url if page == 1 else f"{base_url}page/{page}/"
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        # Find all article URLs on the page
        for h1_tag in soup.find_all('h1', id='list'):
            a_tag = h1_tag.find('a')
            if a_tag and a_tag.get('href'):
                article_urls.append(a_tag['href'])
    
    return article_urls

# Function to translate text to Gujarati with retry mechanism
def translate_to_gujarati(text):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            translator = GoogleTranslator(source='auto', target='gu')
            return translator.translate(text)
        except exceptions.TranslationNotFoundException as e:
            print(f"Translation not found: {e}")
            return text
        except Exception as e:
            print(f"Error in translation (attempt {attempt + 1}): {e}")
            time.sleep(2)  # Wait before retrying
    return text

# Function to split message into chunks
def split_message(message, max_length=4096):
    return [message[i:i+max_length] for i in range(0, len(message), max_length)]

# Function to split content into two parts
def split_content_in_two(content):
    mid_index = len(content) // 2
    for i in range(mid_index, len(content)):
        if content[i] in ['\n', '.', '!', '?']:
            return content[:i + 1], content[i + 1:]
    return content, ""

# Function to scrape the content and send it to the Telegram channel
async def scrape_and_send_to_telegram(url, bot_token, channel_id):
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find the main content div
    main_content = soup.find('div', class_='inside_post column content_width')
    if not main_content:
        raise Exception("Main content div not found")
    
    # Find the heading
    heading = main_content.find('h1', id='list')
    if not heading:
        raise Exception("Heading not found")

    # Prepare the content to be sent
    heading_text = heading.get_text()
    translated_heading = translate_to_gujarati(heading_text)

    message = (
        f"🌟 {translated_heading}\n\n"
        f"🌟 {heading_text}\n\n"
    )

    # Iterate through the sub-tags of the main content
    content = ""
    for tag in main_content.find_all(recursive=False):
        if tag.get('class') == ['sharethis-inline-share-buttons', 'st-center', 'st-has-labels', 'st-inline-share-buttons', 'st-animated']:
            continue

        if tag.get('class') == ['prenext']:
            break

        text = tag.get_text()
        translated_text = translate_to_gujarati(text)

        if tag.name == 'p':
            content += f"🔸 {translated_text}\n\n"
            content += f"🔸 {text}\n\n"
        elif tag.name == 'h2':
            content += f"🔹 {translated_text}\n\n"
            content += f"🔹 {text}\n\n"
        elif tag.name == 'h4':
            content += f"⚡ {translated_text}\n\n"
            content += f"⚡ {text}\n\n"
        elif tag.name == 'ul':
            for li in tag.find_all('li'):
                li_text = li.get_text()
                translated_li_text = translate_to_gujarati(li_text)
                content += f"• {translated_li_text}\n"
                content += f"• {li_text}\n"
            content += "\n"

    part1, part2 = split_content_in_two(content)

    # Add attractive channel promotion for both parts
    promotion = (
        "\n━━━━━━━━━━━━━━━━━━━━\n"
        "🔥 **Stay Updated with the Latest News!** 🔥\n"
        "Join our Telegram channel for:\n"
        "📈 Latest Current Updates\n"
        "📰 Breaking News\n"
        "📚 In-Depth Articles\n"
        "💡 GK \n"
        "\n"
        "👉 [**Join Our Telegram Channel**](https://telegram.me/currentadda) 👈\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
    )

    part1_message = message + part1 + promotion
    part2_message = message + part2 + promotion if part2 else None

    # Split message if it's too long
    chunks_part1 = split_message(part1_message)
    chunks_part2 = split_message(part2_message) if part2_message else []

    # Initialize the bot
    bot = telegram.Bot(token=bot_token)

    # Send each chunk separately for part 1
    for chunk in chunks_part1:
        await bot.send_message(chat_id=channel_id, text=chunk, parse_mode=ParseMode.MARKDOWN)

    # Send each chunk separately for part 2, if it exists
    for chunk in chunks_part2:
        await bot.send_message(chat_id=channel_id, text=chunk, parse_mode=ParseMode.MARKDOWN)

# Function to check if a URL matches the skip pattern
def should_skip_url(url):
    skip_patterns = [
        re.compile(r"daily-current-affairs-quiz-\d{4}/$")  # Matches patterns like daily-current-affairs-quiz-august-7-2024/
    ]
    return any(pattern.search(url) for pattern in skip_patterns)

# Async function to handle main logic
async def main():
    base_url = "https://www.gktoday.in/current-affairs/"
    pages_to_scrape = 2

    # Fetch all article URLs from the specified pages
    article_urls = fetch_article_urls(base_url, pages_to_scrape)

    # Check existing URLs in MongoDB
    existing_urls = set(doc['url'] for doc in collection.find({}, {'_id': 0, 'url': 1}))

    # Filter out URLs that should be skipped
    filtered_urls = [url for url in article_urls if not should_skip_url(url)]

    # Filter out URLs that have already been scraped
    new_urls = [url for url in filtered_urls if url not in existing_urls]

    # Display the new URLs and ask for confirmation to proceed
    print("\nNew URLs to scrape:")
    for idx, url in enumerate(new_urls):
        print(f"{idx + 1}: {url}")
    
    if not new_urls:
        print("No new URLs to scrape.")
        return

    # Send the new URLs to Telegram
    bot_token = TELEGRAM_BOT_TOKEN
    channel_id = TELEGRAM_CHANNEL_USERNAME

    for url in new_urls:
        print(f"Scraping and sending: {url}")
        await scrape_and_send_to_telegram(url, bot_token, channel_id)
        
        # Log the URL to MongoDB after successful scraping
        collection.insert_one({'url': url})

if __name__ == "__main__":
    asyncio.run(main())

