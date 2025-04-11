import pandas as pd
import requests
from bs4 import BeautifulSoup
from Wappalyzer import Wappalyzer, WebPage
from tqdm import tqdm
import concurrent.futures
import time
from urllib.parse import urlparse
import logging
import re
import urllib3
import trafilatura
from langdetect import detect
from transformers import pipeline
from playwright.sync_api import sync_playwright
import asyncio

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Language mapping
LANGUAGE_MAP = {
    'en': 'English',
    'es': 'Spanish',
    'fr': 'French',
    'de': 'German',
    'it': 'Italian',
    'pt': 'Portuguese',
    'nl': 'Dutch',
    'ru': 'Russian',
    'ar': 'Arabic',
    'ja': 'Japanese',
    'ko': 'Korean',
    'zh': 'Chinese',
    'hi': 'Hindi',
    'id': 'Indonesian',
    'vi': 'Vietnamese',
    'th': 'Thai',
    'tr': 'Turkish',
    'pl': 'Polish',
    'uk': 'Ukrainian',
    'cs': 'Czech',
    'sk': 'Slovak',
    'hu': 'Hungarian',
    'ro': 'Romanian',
    'bg': 'Bulgarian',
    'hr': 'Croatian',
    'sr': 'Serbian',
    'sl': 'Slovenian',
    'et': 'Estonian',
    'lv': 'Latvian',
    'lt': 'Lithuanian',
    'el': 'Greek',
    'he': 'Hebrew',
    'fa': 'Persian',
    'ur': 'Urdu',
    'bn': 'Bengali',
    'ta': 'Tamil',
    'te': 'Telugu',
    'ml': 'Malayalam',
    'th': 'Thai',
    'my': 'Burmese',
    'km': 'Khmer',
    'pt-BR': 'Brazilian Portuguese',
    'pt-PT': 'European Portuguese',
    'zh-CN': 'Simplified Chinese',
    'zh-TW': 'Traditional Chinese'
}

# Initialize the summarizer once
summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

def get_dynamic_content(url, timeout=15000):
    """Get content after JavaScript execution using Playwright"""
    try:
        with sync_playwright() as p:
            # Launch browser with shorter timeout
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            )
            page = context.new_page()
            
            # Set shorter timeouts
            page.set_default_timeout(timeout)
            page.set_default_navigation_timeout(timeout)
            
            try:
                # Go to URL with basic load state
                page.goto(url)
                page.wait_for_load_state('domcontentloaded')
                
                # Try to wait for main content, but don't fail if not found
                try:
                    page.wait_for_selector('main, article, #content, .content', timeout=5000)
                except:
                    pass
                
                # Get the page content
                html_content = page.content()
                
                # Get form fields if they exist
                form_fields = []
                try:
                    # Check if form exists without waiting
                    forms = page.locator('form').all()
                    if forms:
                        # Get all input fields and textareas
                        inputs = page.query_selector_all('input[type="text"], input[type="email"], input[type="tel"], textarea')
                        
                        for element in inputs:
                            field_info = {}
                            
                            # Try to get label
                            try:
                                # Try multiple ways to find the label
                                label_id = element.get_attribute('aria-labelledby') or element.get_attribute('id')
                                if label_id:
                                    label = page.query_selector(f'label[for="{label_id}"]')
                                    if label:
                                        field_info['label'] = label.inner_text().strip()
                                else:
                                    # Try finding label as previous sibling
                                    label = page.evaluate('el => el.previousElementSibling?.tagName === "LABEL" ? el.previousElementSibling.textContent : null', element)
                                    if label:
                                        field_info['label'] = label.strip()
                            except:
                                pass
                            
                            # Get placeholder
                            try:
                                placeholder = element.get_attribute('placeholder')
                                if placeholder:
                                    field_info['placeholder'] = placeholder
                            except:
                                pass
                            
                            # Get type
                            try:
                                field_type = element.get_attribute('type')
                                if field_type:
                                    field_info['type'] = field_type
                            except:
                                pass
                            
                            if field_info:
                                form_fields.append(field_info)
                
                except Exception as e:
                    logger.info(f"Error getting form fields: {str(e)}")
                
                return html_content, form_fields
                
            except Exception as e:
                logger.error(f"Error during page navigation: {str(e)}")
                return None, []
            
            finally:
                try:
                    browser.close()
                except:
                    pass
            
    except Exception as e:
        logger.error(f"Error in Playwright setup: {str(e)}")
        return None, []

def get_full_language_name(lang_code):
    """Convert language code to full language name"""
    if not lang_code:
        return "Unknown"
    
    # Handle codes with country variants (e.g., en-US)
    base_code = lang_code.lower().split('-')[0]
    return LANGUAGE_MAP.get(lang_code.lower(), LANGUAGE_MAP.get(base_code, lang_code))

def detect_technologies(soup, response, url):
    """Enhanced technology detection"""
    technologies = []
    
    # Common CMS and platform signatures
    cms_signatures = {
        'WordPress': [
            ('meta', {'name': 'generator', 'content': re.compile('WordPress', re.I)}),
            ('link', {'rel': 'stylesheet', 'href': re.compile('/wp-content/')}),
            ('script', {'src': re.compile('/wp-includes/')}),
        ],
        'Drupal': [
            ('meta', {'name': 'generator', 'content': re.compile('Drupal', re.I)}),
            ('link', {'rel': 'stylesheet', 'href': re.compile('/sites/default/files')}),
        ],
        'Joomla': [
            ('meta', {'name': 'generator', 'content': re.compile('Joomla', re.I)}),
            ('script', {'src': re.compile('/media/jui/')}),
        ],
        'Magento': [
            ('script', {'src': re.compile('/static/frontend/')}),
            ('script', {'src': re.compile('/mage/')}),
        ],
        'Shopify': [
            ('link', {'href': re.compile('.myshopify.com')}),
            ('script', {'src': re.compile('cdn.shopify.com')}),
        ],
        'Wix': [
            ('meta', {'name': 'generator', 'content': re.compile('Wix.com', re.I)}),
            ('script', {'src': re.compile('static.wixstatic.com')}),
        ],
        'Squarespace': [
            ('meta', {'name': 'generator', 'content': re.compile('Squarespace', re.I)}),
            ('script', {'src': re.compile('static[0-9].squarespace.com')}),
        ]
    }
    
    # Check for CMS signatures
    for cms, patterns in cms_signatures.items():
        for tag, attrs in patterns:
            if soup.find(tag, attrs):
                technologies.append(cms)
                break
    
    # Check response headers for technology hints
    headers = response.headers
    if 'x-powered-by' in headers:
        tech = headers['x-powered-by']
        technologies.append(f"Powered by: {tech}")
    
    # Check for common JavaScript frameworks
    js_frameworks = {
        'React': 'react',
        'Vue.js': 'vue',
        'Angular': 'angular',
        'jQuery': 'jquery',
        'Bootstrap': 'bootstrap',
        'Next.js': 'next',
        'Nuxt.js': 'nuxt'
    }
    
    for script in soup.find_all('script', src=True):
        src = script.get('src', '').lower()
        for framework, keyword in js_frameworks.items():
            if keyword in src:
                technologies.append(framework)
    
    # Try Wappalyzer
    try:
        wappalyzer = Wappalyzer.latest()
        webpage = WebPage.new_from_url(url)
        analysis = wappalyzer.analyze_with_categories(webpage)
        
        # Include all detected technologies with their categories
        for tech, categories in analysis.items():
            tech_info = f"{tech} ({', '.join(categories)})"
            technologies.append(tech_info)
    except Exception as e:
        logging.error(f"Wappalyzer error for {url}: {str(e)}")
    
    # Remove duplicates and sort
    technologies = list(set(technologies))
    technologies.sort()
    
    return technologies if technologies else ["No specific platform detected"]

def clean_text(text):
    """Clean and normalize text content"""
    # Remove extra whitespace and newlines
    text = re.sub(r'\s+', ' ', text)
    # Remove special characters but keep basic punctuation
    text = re.sub(r'[^\w\s.,!?-]', '', text)
    return text.strip()

def is_likely_ad_or_irrelevant(element):
    """Check if an element is likely to be an advertisement or irrelevant content"""
    # Common ad-related class names and IDs
    ad_patterns = re.compile(r'(^|-|_)(ad|ads|advert|banner|promo|sponsored|marketing|popup|cookie|newsletter|social-share)', re.I)
    
    # Check element's class and id attributes
    classes = element.get('class', [])
    element_id = element.get('id', '')
    
    if any(ad_patterns.search(cls) for cls in classes) or ad_patterns.search(element_id):
        return True
    
    # Check for common ad-related elements
    if element.find(['iframe', 'ins']):  # 'ins' is often used for ads
        return True
    
    # Check for social media widgets
    social_patterns = re.compile(r'(facebook|twitter|instagram|linkedin|social)', re.I)
    if social_patterns.search(str(element)):
        return True
    
    # Check for newsletter signup forms
    if element.find('form') and any(kw in str(element).lower() for kw in ['subscribe', 'newsletter', 'sign up', 'signup']):
        return True
    
    return False

def get_text_density(element):
    """Calculate the ratio of text length to HTML length"""
    text = element.get_text(strip=True)
    html = str(element)
    if not html:
        return 0
    return len(text) / len(html)

def extract_form_name_from_url(url):
    """Extract form name from URL, handling various formats"""
    try:
        # Remove query parameters and trailing slashes
        clean_url = url.split('?')[0].rstrip('/')
        # Get the last part of the path
        form_name = clean_url.split('/')[-1]
        if form_name and form_name != 'whatsform.com':
            # Clean and format the form name
            form_name = form_name.replace('-', ' ').strip()
            return form_name.title() if form_name else None
    except:
        return None
    return None

def get_homepage_summary(html_content, form_fields=None):
    """Extract and provide a concise summary of the webpage content using BART."""
    try:
        # Try Trafilatura first
        extracted_text = trafilatura.extract(html_content, include_tables=True, include_links=True, include_images=False, no_fallback=False)
        
        if extracted_text and len(extracted_text) > 100:
            logger.info("Using Trafilatura extracted content")
            full_text = extracted_text
        else:
            # Fall back to BeautifulSoup extraction
            logger.info("Falling back to BeautifulSoup extraction")
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Remove only script and style elements
            for element in soup(['script', 'style']):
                element.decompose()
                
            # Get main content areas
            main_content = []
            
            # Get title and h1
            title = soup.title.string.strip() if soup.title else ""
            h1 = soup.find('h1')
            h1_text = h1.get_text().strip() if h1 else ""
            
            if title:
                main_content.append(title)
            if h1_text and h1_text != title:
                main_content.append(h1_text)
                
            # Get meta description
            meta_desc = ""
            meta_tags = soup.find_all('meta', attrs={'name': ['description', 'Description', 'keywords', 'Keywords']})
            meta_tags.extend(soup.find_all('meta', attrs={'property': ['og:description', 'twitter:description']}))
            for meta_tag in meta_tags:
                if 'content' in meta_tag.attrs:
                    content = meta_tag['content'].strip()
                    if content:
                        meta_desc += content + " "
            if meta_desc:
                main_content.append(meta_desc.strip())
            
            # Get all paragraphs and div text
            for tag in ['p', 'div']:
                elements = soup.find_all(tag)
                for element in elements:
                    # Skip navigation and footer elements
                    if element.parent and element.parent.name in ['nav', 'footer', 'header']:
                        continue
                        
                    content = element.get_text(separator=' ', strip=True)
                    if len(content) > 30 and content not in main_content:
                        main_content.append(content)
            
            # Combine all content
            full_text = " ".join(main_content)
            full_text = re.sub(r'\s+', ' ', full_text).strip()
        
        logger.info(f"Total content length before summarization: {len(full_text)} characters")
        
        if len(full_text) < 50:
            logger.warning(f"Short content found ({len(full_text)} chars)")
            return full_text if full_text else "No meaningful content found on the page."
            
        # Use BART for summarization
        try:
            # Chunk the text if it's too long
            max_chunk_length = 1024
            if len(full_text) > max_chunk_length:
                chunks = [full_text[i:i + max_chunk_length] for i in range(0, len(full_text), max_chunk_length)]
                summaries = []
                for chunk in chunks:
                    if len(chunk) >= 50:
                        summary = summarizer(chunk, max_length=150, min_length=50, do_sample=False)
                        summaries.append(summary[0]['summary_text'])
                final_summary = " ".join(summaries)
            else:
                summary = summarizer(full_text, max_length=150, min_length=50, do_sample=False)
                final_summary = summary[0]['summary_text']
                
            # Clean up the summary
            final_summary = re.sub(r'\s+', ' ', final_summary).strip()
            if not final_summary.endswith(('.', '!', '?')):
                final_summary += '.'
                
            return final_summary
            
        except Exception as e:
            logging.error(f"BART summarization failed: {str(e)}")
            # Fall back to extractive method
            sentences = [s.strip() for s in re.split(r'[.!?]+', full_text) if len(s.strip()) > 20][:3]
            return '. '.join(sentences) + '.' if sentences else "Failed to generate summary."
        
    except Exception as e:
        return f"Error extracting summary: {str(e)}"

def is_template_content(text):
    """Check if the text appears to be template content."""
    template_phrases = [
        "Create a form like this",
        "Get data directly from",
        "WhatsForm",
        "WhatsApp number",
        "Create a free form",
        "customer's WhatsApp",
        "Buat form seperti ini",
        "Crie um formulário"
    ]
    return any(phrase.lower() in text.lower() for phrase in template_phrases)

def analyze_single_url(url):
    """Analyze a single URL and return its details"""
    try:
        logger.info(f"Analyzing URL: {url}")
        
        # Get dynamic content using Playwright
        html_content, form_fields = get_dynamic_content(url)
        
        if html_content:
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Detect language
            language = get_full_language_name(soup.html.get('lang', ''))
            logger.info(f"Detected language: {language}")
            
            # Get comprehensive summary using the HTML content
            summary = get_homepage_summary(html_content, form_fields)
            logger.info(f"Generated summary: {summary}")
            
            # Enhanced platform detection
            platforms = detect_technologies(soup, None, url)
            logger.info(f"Detected platforms: {platforms}")
            
            return {
                'url': url,
                'summary': summary,
                'language': language,
                'status_code': 200,
                'platforms': platforms
            }
    except Exception as e:
        logger.error(f"Error analyzing URL {url}: {str(e)}")
        return {
            'url': url,
            'summary': f"Error: {str(e)}",
            'language': 'Unknown',
            'status_code': 'Error',
            'platforms': []
        }

def main():
    """Main function to process URLs from CSV"""
    try:
        logger.info("Reading CSV file...")
        df = pd.read_csv('data - data.csv')
        
        logger.info("Analyzing first 10 rows from CSV...")
        print("\nAnalyzing first 10 rows from CSV...\n")
        
        # Process first 10 rows
        for index, row in df.head(10).iterrows():
            url = row['Form URL']
            logger.info(f"\nProcessing row {index + 1}")
            print(f"\nRow {index + 1}")
            print("=" * 80)
            
            result = analyze_single_url(url)
            
            print(f"URL: {result['url']}")
            print(f"Summary: {result['summary']}")
            print(f"Language: {result['language']}")
            print(f"Status Code: {result['status_code']}")
            print("-" * 80)
            
    except Exception as e:
        logger.error(f"Error in main function: {str(e)}")
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main() 