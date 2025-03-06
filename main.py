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

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='site_analysis.log'
)

# Custom headers to mimic a browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

# Language code to full name mapping
LANGUAGE_MAP = {
    'en': 'English',
    'es': 'Spanish',
    'fr': 'French',
    'de': 'German',
    'it': 'Italian',
    'pt': 'Portuguese',
    'pt-br': 'Brazilian Portuguese',
    'ru': 'Russian',
    'ja': 'Japanese',
    'ko': 'Korean',
    'zh': 'Chinese',
    'ar': 'Arabic',
    'hi': 'Hindi',
    'id': 'Indonesian',
    'nl': 'Dutch',
    'pl': 'Polish',
    'tr': 'Turkish',
    'vi': 'Vietnamese',
    'th': 'Thai',
    'sv': 'Swedish',
    'da': 'Danish',
    'fi': 'Finnish',
    'no': 'Norwegian',
    'cs': 'Czech',
    'el': 'Greek',
    'he': 'Hebrew',
    'ro': 'Romanian',
    'uk': 'Ukrainian',
    'fa': 'Persian',
    'hu': 'Hungarian',
    'sk': 'Slovak',
    'bn': 'Bengali'
}

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

def get_homepage_summary(soup):
    """Extract a comprehensive summary from the homepage"""
    summary_parts = []
    
    # Try meta description first
    meta_desc = soup.find('meta', {'name': 'description'})
    if meta_desc and meta_desc.get('content'):
        summary_parts.append(clean_text(meta_desc['content']))
    
    # Get text from main content areas
    main_content_tags = [
        soup.find('main'),  # Main content area
        soup.find(id=re.compile(r'content|main', re.I)),  # Content by ID
        soup.find(class_=re.compile(r'content|main', re.I))  # Content by class
    ]
    
    # If no main content found, look in the body
    if not any(main_content_tags):
        main_content_tags = [soup.find('body')]
    
    for content in main_content_tags:
        if content:
            # Get text from paragraphs
            for p in content.find_all('p', limit=5):  # Limit to first 5 paragraphs
                text = clean_text(p.get_text())
                if len(text) > 50:  # Only include substantial paragraphs
                    summary_parts.append(text)
            
            # Get text from headings
            for h in content.find_all(['h1', 'h2'], limit=3):  # Main headings
                text = clean_text(h.get_text())
                if text and len(text) > 10:  # Only include meaningful headings
                    summary_parts.append(text)
    
    # Combine all parts and limit length
    summary = ' '.join(summary_parts)
    return summary[:500] if summary else ""  # Increased limit to 500 characters

def analyze_single_url(url):
    """Analyze a single URL and return its properties"""
    try:
        # Make the request
        response = requests.get(url, headers=HEADERS, timeout=10, verify=False)
        
        # Get status code
        status_code = response.status_code
        
        # Initialize results
        summary = ""
        lang = ""
        platform = ""
        
        if status_code == 200:
            # Parse HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Get language
            lang = soup.html.get('lang', '')
            if not lang:
                lang = response.headers.get('content-language', '')
            lang = get_full_language_name(lang)
            
            # Get comprehensive summary
            summary = get_homepage_summary(soup)
            
            # Enhanced platform detection
            technologies = detect_technologies(soup, response, url)
            platform = ' | '.join(technologies)
        
        return {
            'Site_Summary': summary,
            'Site_lang': lang,
            'Site_status_code': status_code,
            'Site_platform': platform
        }
    
    except Exception as e:
        logging.error(f"Error analyzing {url}: {str(e)}")
        return {
            'Site_Summary': "Error fetching",
            'Site_lang': "Unknown",
            'Site_status_code': 0,
            'Site_platform': "Error detecting"
        }

def process_batch(urls, max_workers=5):
    """Process a batch of URLs concurrently"""
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(analyze_single_url, url): url for url in urls}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logging.error(f"Error processing {url}: {str(e)}")
                results.append({
                    'Site_Summary': "Error processing",
                    'Site_lang': "Unknown",
                    'Site_status_code': 0,
                    'Site_platform': "Error"
                })
    return results

def main():
    # Read the CSV file
    print("Reading CSV file...")
    df = pd.read_csv('data - data.csv')
    
    # Get unique URLs to analyze
    urls = df['Form URL'].unique()
    total_urls = len(urls)
    print(f"Found {total_urls} unique URLs to analyze")
    
    # Process URLs in batches
    batch_size = 50
    results = []
    
    for i in tqdm(range(0, total_urls, batch_size), desc="Processing URLs"):
        batch_urls = urls[i:i+batch_size]
        batch_results = process_batch(batch_urls)
        results.extend(batch_results)
        
        # Optional: Save intermediate results every 1000 URLs
        if (i + batch_size) % 1000 == 0:
            print(f"\nSaving intermediate results after processing {i + batch_size} URLs...")
            # Create a mapping of URLs to their results
            temp_url_to_results = {url: result for url, result in zip(urls[:i+batch_size], results)}
            
            # Create a temporary copy of the dataframe
            temp_df = df.copy()
            
            # Update the columns based on Form URL
            for column in ['Site_Summary', 'Site_lang', 'Site_status_code', 'Site_platform']:
                temp_df[column] = temp_df['Form URL'].map(lambda x: temp_url_to_results.get(x, {}).get(column, ''))
            
            # Save intermediate results
            temp_df.to_csv(f'intermediate_results_{i+batch_size}.csv', index=False)
    
    # Create final mapping from URL to results
    url_to_results = {url: result for url, result in zip(urls, results)}
    
    # Update the DataFrame columns
    print("\nUpdating DataFrame with results...")
    for column in ['Site_Summary', 'Site_lang', 'Site_status_code', 'Site_platform']:
        df[column] = df['Form URL'].map(lambda x: url_to_results.get(x, {}).get(column, ''))
    
    # Save the final results
    print("Saving final results...")
    df.to_csv('updated_data.csv', index=False)
    print("Done! Results have been saved to 'updated_data.csv'")

if __name__ == "__main__":
    main() 