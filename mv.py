import time
import re
from botasaurus.browser import browser, Driver
from botasaurus.soupify import soupify
from supabase import create_client, Client

# --- KONFIGURASI SUPABASE ---
SUPABASE_URL = "https://your-project-id.supabase.co"
SUPABASE_KEY = "your-service-role-key"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

@browser(
    headless=False,           # Set ke True jika ingin berjalan tanpa jendela browser
    reuse_driver=True,
    block_images=True,        
    window_size=(1280, 720)
)
def run_latest_scraper_supabase(driver: Driver, data=None):
    # Sesuai format yang kamu berikan: https://tv10.lk21official.cc/latest/page/1
    base_url_pattern = "https://tv10.lk21official.cc/latest/page/"
    END_PAGE = 10  
    
    print(f"[*] MEMULAI SCRAPING - TARGET: 10 HALAMAN")

    for page in range(1, END_PAGE + 1):
        target_url = f"{base_url_pattern}{page}"
        print(f"\n--- SCANNING HALAMAN {page}: {target_url} ---")
        
        driver.get(target_url)
        time.sleep(8) # Tunggu bypass Cloudflare
        
        page_soup = soupify(driver)
        articles = page_soup.select('article')
        
        movie_list = []
        for art in articles:
            a_tag = art.select_one('figure a')
            year_tag = art.select_one('span.year')
            
            if a_tag and year_tag:
                href = a_tag.get('href')
                full_url = href if href.startswith('http') else "https://tv10.lk21official.cc" + href
                
                # Filter agar tidak mengambil link non-film (seperti genre/year link)
                if '/year/' not in full_url and '/genre/' not in full_url:
                    slug = full_url.strip('/').split('/')[-1]
                    year_text = year_tag.get_text(strip=True)
                    year_val = re.sub(r'\D', '', year_text)
                    
                    movie_list.append({
                        'url': full_url, 
                        'slug': slug, 
                        'year': int(year_val) if year_val else None
                    })

        print(f"    [+] Terdeteksi {len(movie_list)} film di halaman ini.")

        # --- PROSES DETAIL & UPSERT ---
        for movie in movie_list:
            slug = movie['slug']
            try:
                print(f"        [*] Scraping Detail: {slug}")
                driver.get(movie['url'])
                time.sleep(7) 
                
                detail_soup = soupify(driver)
                options = detail_soup.select('select#player-select option')
                
                found_providers = {}
                for opt in options:
                    val = opt.get('value', '').strip()
                    server = opt.get('data-server', '').lower()
                    if val:
                        # Membersihkan URL Iframe
                        clean_link = 'https:' + val if val.startswith('//') else \
                                     'https://playeriframe.sbs' + val if val.startswith('/') else val
                        found_providers[server] = clean_link

                # Payload sesuai dengan schema public.movies kamu
                if found_providers:
                    payload = {
                        "slug": slug,
                        "year": movie['year'],
                        "link_cast": found_providers.get('cast', ''),
                        "link_turbo": found_providers.get('turbovip', '')
                    }

                    # Eksekusi UPSERT (Update jika slug sudah ada, Insert jika baru)
                    try:
                        supabase.table("movies").upsert(
                            payload, 
                            on_conflict="slug"
                        ).execute()
                        print(f"            [OK] Berhasil Upsert ke Supabase.")
                    except Exception as db_err:
                        print(f"            [!] Gagal Database: {db_err}")
                else:
                    print(f"            [SKIP] Tidak ada link player.")

            except Exception as e:
                print(f"            [ERROR] Gagal pada movie {slug}: {e}")

    print(f"\n[DONE] Scraping selesai hingga halaman {END_PAGE}.")

if __name__ == "__main__":
    run_latest_scraper_supabase()
