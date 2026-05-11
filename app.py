import os
import json
import re
from pathlib import Path
import streamlit as st
import pandas as pd
import textwrap

def mc_to_html(text):
    if "§" not in text:
        return text
        
    MC_COLORS = {
        '0': '#000000', '1': '#0000AA', '2': '#00AA00', '3': '#00AAAA',
        '4': '#AA0000', '5': '#AA00AA', '6': '#FFAA00', '7': '#AAAAAA',
        '8': '#555555', '9': '#5555FF', 'a': '#55FF55', 'b': '#55FFFF',
        'c': '#FF5555', 'd': '#FF55FF', 'e': '#FFFF55', 'f': '#FFFFFF'
    }
    
    html = "<span>"
    i = 0
    while i < len(text):
        if text[i] == '§' and i + 1 < len(text):
            code = text[i+1].lower()
            if code in MC_COLORS:
                html += f'</span><span style="color: {MC_COLORS[code]}">'
            elif code == 'r':
                html += '</span><span>'
            i += 2
        else:
            html += text[i]
            i += 1
            
    html += "</span>"
    return html

def format_perks(perk_array):
    if not perk_array:
        return "No known perks"
    lines = []
    for item in perk_array:
        parts = item.split(':::', 1)
        if len(parts) == 2:
            name, desc = parts
            wrapped_desc = "<br>".join(textwrap.wrap(desc, width=60))
            colored_name = mc_to_html(name)
            colored_desc = mc_to_html(wrapped_desc)
            lines.append(f"<b>{colored_name}</b>:<br>{colored_desc}")
    return "<br><br>".join(lines)
import psycopg2
import plotly.express as px
import cloudscraper
from bs4 import BeautifulSoup
from dotenv import load_dotenv

ITEMS_DIR = Path(os.path.abspath(__file__)).parent.parent / "NotEnoughUpdates-REPO" / "items"

# Set page configuration
st.set_page_config(
    page_title="Hypixel Bazaar Dashboard",
    page_icon="📈",
    layout="wide"
)

with st.sidebar:
    if st.button("Clear App Cache"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.success("Cache cleared! Reloading...")
        st.rerun()

# Load environment variables
load_dotenv()

def _validate_connection(conn):
    if conn.closed != 0:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        return True
    except Exception:
        return False

@st.cache_resource(validate=_validate_connection)
def get_database_connection():
    database_url = os.getenv("DATABASE_URL_YARON")
    if not database_url:
        st.error("DATABASE_URL_YARON is not set in .env")
        st.stop()
    try:
        conn = psycopg2.connect(database_url)
        conn.autocommit = True
        return conn
    except psycopg2.OperationalError as e:
        st.error(f"Failed to connect to database: {e}")
        st.stop()

conn = get_database_connection()

MAYOR_COLORS = {
    "Finnegan": "#2ECC71",
    "Paul": "#E74C3C",
    "Aatrox": "#F1C40F",
    "Cole": "#27AE60",
    "Diana": "#9B59B6",
    "Marina": "#3498DB",
    "Foxy": "#E67E22",
    "Derpy": "#FF69B4",
    "Scorpius": "#FF69B4",
    "Jerry": "#FF69B4",
}

@st.cache_data(ttl=600)
def get_mayor_history():
    query = """
    WITH unpivoted AS (
        SELECT year, candidate_1 AS candidate, candidate_1_votes AS votes FROM election
        UNION ALL SELECT year, candidate_2, candidate_2_votes FROM election
        UNION ALL SELECT year, candidate_3, candidate_3_votes FROM election
        UNION ALL SELECT year, candidate_4, candidate_4_votes FROM election
        UNION ALL SELECT year, candidate_5, candidate_5_votes FROM election
    ),
    ranked AS (
        SELECT year, candidate, votes, ROW_NUMBER() OVER(PARTITION BY year ORDER BY votes DESC) as rn
        FROM unpivoted
        WHERE candidate IS NOT NULL
    ),
    election_results AS (
        SELECT 
            r1.year,
            r1.candidate AS mayor,
            r2.candidate AS minister
        FROM ranked r1
        LEFT JOIN ranked r2 ON r1.year = r2.year AND r2.rn = 2
        WHERE r1.rn = 1
    ),
    mayor_perks AS (
        SELECT election_year, mayor, ARRAY_AGG('• ' || perk_name || ':::' || description) as m_perks
        FROM perks_election
        GROUP BY election_year, mayor
    ),
    minister_perks AS (
        SELECT election_year, mayor as minister, ARRAY_AGG('• ' || perk_name || ':::' || description) as min_perks
        FROM perks_election
        WHERE minister = true
        GROUP BY election_year, mayor
    ),
    election_full AS (
        SELECT 
            e.year,
            e.mayor,
            mp.m_perks,
            e.minister,
            minp.min_perks
        FROM election_results e
        LEFT JOIN mayor_perks mp ON e.year = mp.election_year AND e.mayor = mp.mayor
        LEFT JOIN minister_perks minp ON e.year = minp.election_year AND e.minister = minp.minister
    )
    SELECT 
        s.skyblock_year, 
        MIN(s.collected_at) as start_time, 
        MAX(s.collected_at) as end_time,
        f.mayor as active_mayor,
        f.minister as active_minister,
        f.m_perks,
        f.min_perks
    FROM snapshots s
    LEFT JOIN election_full f ON f.year = s.skyblock_year
    GROUP BY s.skyblock_year, f.mayor, f.minister, f.m_perks, f.min_perks
    ORDER BY s.skyblock_year;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            cols = [desc[0] for desc in cur.description]
            return pd.DataFrame(cur.fetchall(), columns=cols)
    except Exception as e:
        st.error(f"Error fetching mayor history: {e}")
        return pd.DataFrame()

st.title("📈 Hypixel SkyBlock Dashboard")

@st.cache_data(ttl=86400)
def get_item_data(product_id, fetch_icon=True):
    result = {"displayname": product_id, "icon_url": None}
    
    json_path = ITEMS_DIR / f"{product_id}.json"
    if not json_path.exists():
        return result
        
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        displayname = data.get("displayname", product_id)
        result["displayname"] = re.sub(r'§[0-9a-fk-or]', '', displayname)
        
        if not fetch_icon:
            return result
            
        info_urls = data.get("info", [])
        if not isinstance(info_urls, list):
            return result
            
        fandom_url = next((url for url in info_urls if "fandom.com" in url), None)
        
        if not fandom_url:
            return result
        
        try:
            # Try with cloudscraper first
            scraper = cloudscraper.create_scraper()
            html = scraper.get(fandom_url, timeout=10).text
            soup = BeautifulSoup(html, 'html.parser')
            
            img_meta = soup.find('meta', property='og:image')
            if img_meta and img_meta.get('content'):
                result["icon_url"] = img_meta['content']
                return result
        except Exception as scraper_error:
            # Fallback: try with plain requests
            try:
                import requests
                response = requests.get(fandom_url, timeout=10)
                soup = BeautifulSoup(response.text, 'html.parser')
                img_meta = soup.find('meta', property='og:image')
                if img_meta and img_meta.get('content'):
                    result["icon_url"] = img_meta['content']
                    return result
            except Exception as requests_error:
                print(f"[Icon Fetch] Failed for {product_id}: cloudscraper={scraper_error}, requests={requests_error}")
            
        return result
    except Exception as e:
        import traceback
        print(f"[Icon Fetch Error] {product_id}: {e}")
        return result

@st.cache_data(ttl=86400)
def get_recipe_image_map(product_id):
    img_map = {}
    json_path = ITEMS_DIR / f"{product_id}.json"
    if not json_path.exists():
        return img_map
        
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        info_urls = data.get("info", [])
        if not isinstance(info_urls, list):
            return img_map
            
        fandom_url = next((url for url in info_urls if "fandom.com" in url), None)
        if not fandom_url:
            return img_map
        
        html = None
        try:
            # Try with cloudscraper first
            scraper = cloudscraper.create_scraper()
            html = scraper.get(fandom_url, timeout=10).text
        except Exception as scraper_error:
            # Fallback: try with plain requests
            try:
                import requests
                response = requests.get(fandom_url, timeout=10)
                html = response.text
            except Exception as requests_error:
                print(f"[Recipe Images] Failed for {product_id}: cloudscraper={scraper_error}, requests={requests_error}")
                return img_map
        
        if not html:
            return img_map
            
        soup = BeautifulSoup(html, 'html.parser')
        
        for img in soup.find_all('img'):
            alt = img.get('alt')
            if not alt: continue
            src = img.get('data-src') or img.get('src')
            if not src or src.startswith('data:image'): continue
            
            alt_clean = alt.replace('.png', '').replace('.gif', '').strip()
            raw_src = src.split('/scale-to-width-down/')[0]
            if '?' in raw_src:
                raw_src = raw_src.split('?')[0]
                
            img_map[alt_clean] = raw_src
            
        return img_map
    except Exception as e:
        print(f"[Recipe Images Error] {product_id}: {e}")
        return img_map

def render_crafting_table(recipe_slots, result_id, result_amount):
    html = """
    <style>
    .mc-gui {
        background-color: #c6c6c6;
        border: 4px solid #555555;
        border-bottom-color: #ffffff;
        border-right-color: #ffffff;
        padding: 20px;
        display: inline-flex;
        align-items: center;
        gap: 30px;
        font-family: monospace;
        border-radius: 4px;
    }
    .mc-grid {
        display: grid;
        grid-template-columns: repeat(3, 44px);
        grid-template-rows: repeat(3, 44px);
        gap: 2px;
    }
    .mc-slot {
        width: 44px;
        height: 44px;
        background-color: #8b8b8b;
        border: 2px solid #373737;
        border-bottom-color: #ffffff;
        border-right-color: #ffffff;
        position: relative;
    }
    .mc-slot img {
        width: 36px;
        height: 36px;
        position: absolute;
        top: 2px;
        left: 2px;
        object-fit: contain;
    }
    .mc-qty {
        position: absolute;
        bottom: -2px;
        right: 2px;
        color: white;
        text-shadow: 1px 1px 0 #000, -1px -1px 0 #000, 1px -1px 0 #000, -1px 1px 0 #000;
        font-size: 14px;
        font-weight: bold;
    }
    .mc-arrow {
        font-size: 40px;
        color: #8b8b8b;
        text-shadow: 2px 2px 0 #373737;
    }
    .mc-result-slot {
        width: 64px;
        height: 64px;
        background-color: #8b8b8b;
        border: 2px solid #373737;
        border-bottom-color: #ffffff;
        border-right-color: #ffffff;
        position: relative;
    }
    .mc-result-slot img {
        width: 52px;
        height: 52px;
        position: absolute;
        top: 4px;
        left: 4px;
        object-fit: contain;
    }
    </style>
    <div class="mc-gui">
        <div class="mc-grid">
    """
    
    img_map = get_recipe_image_map(result_id)
    
    slot_names = ['A1', 'A2', 'A3', 'B1', 'B2', 'B3', 'C1', 'C2', 'C3']
    for slot in slot_names:
        item_id, qty = recipe_slots.get(slot, (None, 0))
        if item_id:
            data = get_item_data(item_id, fetch_icon=False)
            name = data['displayname']
            
            icon = img_map.get(name)
            if not icon:
                data_fallback = get_item_data(item_id, fetch_icon=True)
                icon = data_fallback['icon_url']
                
            img_tag = f'<img src="{icon}" title="{name}">' if icon else f'<div title="{name}" style="width:100%;height:100%;display:flex;align-items:center;justify-content:center;font-size:10px;color:black;">?</div>'
            qty_tag = f'<div class="mc-qty">{qty}</div>' if qty > 1 else ''
            html += f'<div class="mc-slot" title="{name}">{img_tag}{qty_tag}</div>'
        else:
            html += '<div class="mc-slot"></div>'
            
    html += """
        </div>
        <div class="mc-arrow">➔</div>
    """
    
    result_data = get_item_data(result_id, fetch_icon=False)
    r_name = result_data['displayname']
    
    r_icon = img_map.get(r_name)
    if not r_icon:
        r_icon = get_item_data(result_id, fetch_icon=True)['icon_url']
        
    r_img_tag = f'<img src="{r_icon}" title="{r_name}">' if r_icon else f'<div title="{r_name}" style="width:100%;height:100%;display:flex;align-items:center;justify-content:center;font-size:14px;color:black;">?</div>'
    r_qty_tag = f'<div class="mc-qty" style="font-size:16px;">{int(result_amount)}</div>' if result_amount > 1 else ''
    
    html += f'<div class="mc-result-slot" title="{r_name}">{r_img_tag}{r_qty_tag}</div></div>'
    return html

# Create tabs for navigation
tab1, tab2, tab3 = st.tabs(["📊 Bazaar Prices", "🗳️ Mayor Elections", "💰 Crafting Profits"])

# --- TAB 1: Bazaar Prices ---
with tab1:
    st.header("Bazaar Price Trends")
    
    # Fetch available products
    @st.cache_data(ttl=600)
    def get_products():
        query = """
        SELECT product_id 
        FROM bazaar_prices 
        WHERE snapshot_id = (SELECT id FROM snapshots ORDER BY collected_at DESC LIMIT 1)
          AND (buy_volume > 0 OR sell_volume > 0)
        ORDER BY product_id;
        """
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                return [row[0] for row in cur.fetchall()]
        except psycopg2.errors.UndefinedTable:
            return []
    
    products = get_products()
    
    if products:
        selected_product = st.selectbox(
            "Select an Item", 
            products, 
            index=0,
            format_func=lambda x: f'{get_item_data(x, fetch_icon=False)["displayname"]} ({x})'
        )
        
        # --- RECIPE VIEWER ---
        st.subheader("Crafting Recipe")
        query_recipe = """
        SELECT output_amount, 
               a1, a1_quantity, a2, a2_quantity, a3, a3_quantity,
               b1, b1_quantity, b2, b2_quantity, b3, b3_quantity,
               c1, c1_quantity, c2, c2_quantity, c3, c3_quantity
        FROM item_recipes
        WHERE product_id = %s;
        """
        try:
            with conn.cursor() as cur:
                cur.execute(query_recipe, (selected_product,))
                recipe_row = cur.fetchone()
                
            if recipe_row:
                output_amount = recipe_row[0]
                slots = {
                    'A1': (recipe_row[1], recipe_row[2]),
                    'A2': (recipe_row[3], recipe_row[4]),
                    'A3': (recipe_row[5], recipe_row[6]),
                    'B1': (recipe_row[7], recipe_row[8]),
                    'B2': (recipe_row[9], recipe_row[10]),
                    'B3': (recipe_row[11], recipe_row[12]),
                    'C1': (recipe_row[13], recipe_row[14]),
                    'C2': (recipe_row[15], recipe_row[16]),
                    'C3': (recipe_row[17], recipe_row[18]),
                }
                html_table = render_crafting_table(slots, selected_product, output_amount)
                st.markdown(html_table, unsafe_allow_html=True)
            else:
                st.info("No crafting recipe available for this item.")
        except psycopg2.errors.UndefinedTable:
            st.info("No crafting recipe available for this item.")
            
        st.divider()
        
        # Fetch price data for selected product
        query = """
        SELECT s.collected_at, bp.buy_price, bp.sell_price, bp.buy_volume, bp.sell_volume
        FROM bazaar_prices bp
        JOIN snapshots s ON bp.snapshot_id = s.id
        WHERE bp.product_id = %s
        ORDER BY s.collected_at ASC;
        """
        
        with conn.cursor() as cur:
            cur.execute(query, (selected_product,))
            rows = cur.fetchall()
            columns = [desc[0] for desc in cur.description]
            df_prices = pd.DataFrame(rows, columns=columns)
        
        if not df_prices.empty:
            df_prices['collected_at'] = pd.to_datetime(df_prices['collected_at'])
            
            # Plotly Line Chart for Prices
            fig_prices = px.line(
                df_prices, 
                x='collected_at', 
                y=['buy_price', 'sell_price'],
                labels={'value': 'Coins', 'collected_at': 'Time', 'variable': 'Price Type'},
                title=f"Price History for {selected_product}",
                template="plotly_dark"
            )
            fig_prices.update_layout(dragmode='pan')
            fig_prices.update_yaxes(minallowed=0)
            
            mayor_df = get_mayor_history()
            def apply_mayors(fig):
                if mayor_df.empty:
                    return
                for _, row in mayor_df.iterrows():
                    mayor = row['active_mayor']
                    minister = row['active_minister']
                    m_perks_text = format_perks(row['m_perks'])
                    min_perks_text = format_perks(row['min_perks'])
                    start = row['start_time']
                    end = row['end_time']
                    
                    if pd.isna(mayor):
                        continue
                        
                    color = MAYOR_COLORS.get(mayor, "#888888")
                    
                    label_text = f"👑 {mayor}"
                    if not pd.isna(minister):
                        label_text += f"<br>👔 {minister}"
                        
                    hover_text = f"<b>👑 Mayor {mayor}</b><br>{m_perks_text}"
                    if not pd.isna(minister):
                        hover_text += f"<br><br><b>👔 Minister {minister}</b><br>{min_perks_text}"

                    mid_time = start + (end - start) / 2
                    fig.add_vrect(
                        x0=start, x1=end,
                        fillcolor=color, opacity=0.15,
                        layer="below", line_width=0
                    )
                    fig.add_annotation(
                        x=mid_time,
                        y=1, yref="paper",
                        yanchor="top", xanchor="center",
                        text=label_text,
                        font=dict(color=color),
                        hovertext=hover_text,
                        showarrow=False
                    )
                    fig.add_shape(
                        type="rect",
                        x0=start, x1=end,
                        y0=0, y1=0.03,
                        yref="paper",
                        fillcolor=color, opacity=1.0,
                        layer="below", line_width=0
                    )
                    
            apply_mayors(fig_prices)
            st.plotly_chart(fig_prices, use_container_width=True, config={'scrollZoom': True})
            
            # Metrics
            st.subheader("Current Metrics")
            latest_data = df_prices.iloc[-1]
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Latest Buy Price", f"{latest_data['buy_price']:,.1f}")
            col2.metric("Latest Sell Price", f"{latest_data['sell_price']:,.1f}")
            col3.metric("Buy Volume", f"{latest_data['buy_volume']:,}")
            col4.metric("Sell Volume", f"{latest_data['sell_volume']:,}")
            
            # Plotly Area Chart for Volume
            fig_volume = px.area(
                df_prices,
                x='collected_at',
                y=['buy_volume', 'sell_volume'],
                labels={'value': 'Volume', 'collected_at': 'Time', 'variable': 'Volume Type'},
                title=f"Volume History for {selected_product}",
                template="plotly_dark"
            )
            fig_volume.update_layout(dragmode='pan')
            fig_volume.update_yaxes(minallowed=0)
            apply_mayors(fig_volume)
            st.plotly_chart(fig_volume, use_container_width=True, config={'scrollZoom': True})
            
            # --- CRAFTING PROFIT ANALYSIS FOR THIS ITEM ---
            st.divider()
            st.subheader("💰 Crafting Profit Analysis")
            
            # Get recipe for this item
            recipe_query = """
            SELECT output_amount, 
                   a1, a1_quantity, a2, a2_quantity, a3, a3_quantity,
                   b1, b1_quantity, b2, b2_quantity, b3, b3_quantity,
                   c1, c1_quantity, c2, c2_quantity, c3, c3_quantity
            FROM item_recipes
            WHERE product_id = %s;
            """
            try:
                with conn.cursor() as cur:
                    cur.execute(recipe_query, (selected_product,))
                    recipe_row = cur.fetchone()
                    
                if recipe_row:
                    output_amount = recipe_row[0]
                    
                    # Build ingredients map
                    ingredients = {}
                    ingredient_positions = [
                        (recipe_row[1], recipe_row[2]),
                        (recipe_row[3], recipe_row[4]),
                        (recipe_row[5], recipe_row[6]),
                        (recipe_row[7], recipe_row[8]),
                        (recipe_row[9], recipe_row[10]),
                        (recipe_row[11], recipe_row[12]),
                        (recipe_row[13], recipe_row[14]),
                        (recipe_row[15], recipe_row[16]),
                        (recipe_row[17], recipe_row[18]),
                    ]
                    
                    for item_id, qty in ingredient_positions:
                        if item_id:
                            ingredients[item_id] = ingredients.get(item_id, 0) + qty
                    
                    if ingredients:
                        # Get all ingredient IDs
                        ingredient_ids = list(ingredients.keys())
                        
                        # Query all prices for result and ingredients
                        placeholders = ','.join(['%s'] * (len(ingredient_ids) + 1))
                        price_query = f"""
                        SELECT s.collected_at, bp.product_id, bp.buy_price, bp.sell_price
                        FROM bazaar_prices bp
                        JOIN snapshots s ON bp.snapshot_id = s.id
                        WHERE bp.product_id IN ({placeholders})
                        ORDER BY s.collected_at ASC;
                        """
                        
                        params = [selected_product] + ingredient_ids
                        with conn.cursor() as cur:
                            cur.execute(price_query, params)
                            price_rows = cur.fetchall()
                        
                        if price_rows:
                            # Organize prices by timestamp and item
                            prices_by_time = {}
                            for collected_at, item_id, buy_price, sell_price in price_rows:
                                if collected_at not in prices_by_time:
                                    prices_by_time[collected_at] = {}
                                prices_by_time[collected_at][item_id] = (buy_price, sell_price)
                            
                            # Calculate profit for each timestamp
                            profits = []
                            for timestamp in sorted(prices_by_time.keys()):
                                prices = prices_by_time[timestamp]
                                
                                # Check if we have all necessary prices
                                if selected_product not in prices:
                                    continue
                                
                                # Check if we have all ingredient prices
                                has_all_ingredients = all(ing_id in prices for ing_id in ingredient_ids)
                                if not has_all_ingredients:
                                    continue
                                
                                # Calculate costs
                                total_cost = 0
                                for ing_id, qty in ingredients.items():
                                    buy_price, _ = prices[ing_id]
                                    total_cost += buy_price * qty
                                
                                # Calculate revenue
                                _, sell_price = prices[selected_product]
                                revenue = sell_price * output_amount
                                
                                # Calculate profit
                                profit = revenue - total_cost
                                
                                profits.append({
                                    'collected_at': timestamp,
                                    'profit': profit,
                                    'output_amount': output_amount,
                                    'result_sell_price': sell_price,
                                    'total_cost': total_cost,
                                    'revenue': revenue
                                })
                            
                            if profits:
                                profit_df = pd.DataFrame(profits)
                                profit_df['collected_at'] = pd.to_datetime(profit_df['collected_at'])
                                
                                # Metrics
                                latest_profit = profit_df.iloc[-1]
                                col1, col2, col3, col4 = st.columns(4)
                                
                                profit_color_emoji = "🟢" if latest_profit['profit'] > 0 else "🔴"
                                col1.metric(
                                    f"{profit_color_emoji} Profit per Craft",
                                    f"{latest_profit['profit']:,.0f} coins",
                                    delta=f"Output: {int(latest_profit['output_amount'])} items"
                                )
                                col2.metric("Selling Price", f"{latest_profit['result_sell_price']:,.1f} coins")
                                col3.metric("Total Cost", f"{latest_profit['total_cost']:,.0f} coins")
                                col4.metric("Profit Margin %", f"{(latest_profit['profit'] / max(latest_profit['total_cost'], 1) * 100):.1f}%")
                                
                                # Profit Chart
                                fig_profit = px.line(
                                    profit_df,
                                    x='collected_at',
                                    y='profit',
                                    labels={'collected_at': 'Time', 'profit': 'Profit (coins)'},
                                    title=f"Crafting Profit Over Time",
                                    template="plotly_dark"
                                )
                                fig_profit.update_layout(dragmode='pan', hovermode='x unified')
                                fig_profit.update_traces(
                                    line=dict(color='#2ECC71'),
                                    fillcolor='rgba(46, 204, 113, 0.2)',
                                    fill='tozeroy'
                                )
                                fig_profit.update_yaxes(rangemode='normal')
                                
                                mayor_df = get_mayor_history()
                                def apply_mayors_craft(fig):
                                    if mayor_df.empty:
                                        return
                                    for _, row in mayor_df.iterrows():
                                        mayor = row['active_mayor']
                                        start = row['start_time']
                                        end = row['end_time']
                                        if pd.isna(mayor):
                                            continue
                                        color = MAYOR_COLORS.get(mayor, "#888888")
                                        fig.add_vrect(x0=start, x1=end, fillcolor=color, opacity=0.1, layer="below", line_width=0)
                                
                                apply_mayors_craft(fig_profit)
                                st.plotly_chart(fig_profit, use_container_width=True, config={'scrollZoom': True})
                                
                                # Cost Breakdown Chart
                                fig_breakdown = px.line(
                                    profit_df,
                                    x='collected_at',
                                    y=['result_sell_price', 'total_cost'],
                                    labels={'collected_at': 'Time', 'value': 'Coins', 'variable': 'Type'},
                                    title="Recipe Cost vs Selling Price",
                                    template="plotly_dark"
                                )
                                fig_breakdown.update_layout(dragmode='pan', hovermode='x unified')
                                apply_mayors_craft(fig_breakdown)
                                st.plotly_chart(fig_breakdown, use_container_width=True, config={'scrollZoom': True})
                            else:
                                st.info("This item has a recipe but insufficient price history for crafting analysis.")
                        else:
                            st.info("No price data available for this recipe's ingredients.")
                    else:
                        st.info("This recipe has no ingredients.")
                else:
                    st.info("This item does not have a crafting recipe available.")
            except psycopg2.errors.UndefinedTable:
                st.info("No crafting recipe available for this item.")
            
        else:
            st.info("No price data available for this item.")
    else:
        st.warning("No products found in the database.")


# --- TAB 2: Mayor Elections ---
with tab2:
    st.header("Mayor Election History")
    
    @st.cache_data(ttl=600)
    def get_elections():
        query = """
        SELECT year, candidate_1, candidate_1_votes, candidate_2, candidate_2_votes,
               candidate_3, candidate_3_votes, candidate_4, candidate_4_votes,
               candidate_5, candidate_5_votes
        FROM election
        ORDER BY year DESC;
        """
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                columns = [desc[0] for desc in cur.description]
                return pd.DataFrame(rows, columns=columns)
        except psycopg2.errors.UndefinedTable:
            return pd.DataFrame()
            
    df_elections = get_elections()
    
    if not df_elections.empty:
        years = df_elections['year'].tolist()
        selected_year = st.selectbox("Select Election Year", years)
        
        election_data = df_elections[df_elections['year'] == selected_year].iloc[0]
        
        # Prepare data for plotting
        candidates = []
        votes = []
        for i in range(1, 6):
            candidate = election_data[f'candidate_{i}']
            vote_count = election_data[f'candidate_{i}_votes']
            if candidate and pd.notna(vote_count):
                candidates.append(candidate)
                votes.append(vote_count)
                
        query_perks = """
        SELECT mayor, ARRAY_AGG((CASE WHEN minister THEN '👔 ' ELSE '• ' END) || perk_name || ':::' || description) as perks
        FROM perks_election
        WHERE election_year = %s
        GROUP BY mayor;
        """
        with conn.cursor() as cur:
            cur.execute(query_perks, (selected_year,))
            perks_rows = cur.fetchall()
            perks_dict = {row[0]: format_perks(row[1]) for row in perks_rows}
            
        hover_texts = []
        for candidate in candidates:
            hover_texts.append(perks_dict.get(candidate, "No perks found."))
                
        df_votes = pd.DataFrame({
            'Candidate': candidates, 
            'Votes': votes,
            'Hover': hover_texts
        })
        df_votes = df_votes.sort_values(by='Votes', ascending=True)
        
        fig_votes = px.bar(
            df_votes,
            x='Votes',
            y='Candidate',
            orientation='h',
            title=f"Election Results (Year {selected_year})",
            template="plotly_dark",
            text='Votes',
            color='Candidate',
            color_discrete_map=MAYOR_COLORS,
            custom_data=['Hover']
        )
        fig_votes.update_traces(
            texttemplate='%{text:,.0f}', 
            textposition='outside',
            hovertemplate='<b>%{y}</b><br>Votes: %{x:,.0f}<br><br>%{customdata[0]}<extra></extra>'
        )
        fig_votes.update_layout(showlegend=False)
        
        st.plotly_chart(fig_votes, use_container_width=True, config={'scrollZoom': True})
                
    else:
        st.warning("No election data found in the database.")


# --- TAB 3: Crafting Profits Ranking ---
with tab3:
    st.header("💰 Crafting Profit Rankings")
    st.markdown("All craftable items ranked by current profit (most recent data)")

    @st.cache_data(ttl=600)
    def calculate_profit_over_time(product_id):
        """Calculate profit/loss over time for a given recipe item"""
        recipe_query = """
        SELECT output_amount,
               a1, a1_quantity, a2, a2_quantity, a3, a3_quantity,
               b1, b1_quantity, b2, b2_quantity, b3, b3_quantity,
               c1, c1_quantity, c2, c2_quantity, c3, c3_quantity
        FROM item_recipes
        WHERE product_id = %s;
        """

        with conn.cursor() as cur:
            cur.execute(recipe_query, (product_id,))
            recipe_row = cur.fetchone()

        if not recipe_row:
            return pd.DataFrame()

        output_amount = recipe_row[0]

        ingredients = {}
        ingredient_positions = [
            (recipe_row[1], recipe_row[2]),
            (recipe_row[3], recipe_row[4]),
            (recipe_row[5], recipe_row[6]),
            (recipe_row[7], recipe_row[8]),
            (recipe_row[9], recipe_row[10]),
            (recipe_row[11], recipe_row[12]),
            (recipe_row[13], recipe_row[14]),
            (recipe_row[15], recipe_row[16]),
            (recipe_row[17], recipe_row[18]),
        ]

        for item_id, qty in ingredient_positions:
            if item_id:
                ingredients[item_id] = ingredients.get(item_id, 0) + qty

        if not ingredients:
            return pd.DataFrame()

        ingredient_ids = list(ingredients.keys())
        placeholders = ','.join(['%s'] * (len(ingredient_ids) + 1))
        price_query = f"""
        SELECT s.collected_at, bp.product_id, bp.buy_price, bp.sell_price
        FROM bazaar_prices bp
        JOIN snapshots s ON bp.snapshot_id = s.id
        WHERE bp.product_id IN ({placeholders})
        ORDER BY s.collected_at ASC;
        """

        params = [product_id] + ingredient_ids
        with conn.cursor() as cur:
            cur.execute(price_query, params)
            price_rows = cur.fetchall()

        if not price_rows:
            return pd.DataFrame()

        prices_by_time = {}
        for collected_at, item_id, buy_price, sell_price in price_rows:
            if collected_at not in prices_by_time:
                prices_by_time[collected_at] = {}
            prices_by_time[collected_at][item_id] = (buy_price, sell_price)

        profits = []
        for timestamp in sorted(prices_by_time.keys()):
            prices = prices_by_time[timestamp]
            if product_id not in prices:
                continue
            if not all(ing_id in prices for ing_id in ingredient_ids):
                continue

            total_cost = 0
            for ing_id, qty in ingredients.items():
                buy_price, _ = prices[ing_id]
                total_cost += buy_price * qty

            _, sell_price = prices[product_id]
            revenue = sell_price * output_amount
            profit = revenue - total_cost

            profits.append({
                'collected_at': timestamp,
                'profit': profit,
                'output_amount': output_amount,
                'result_sell_price': sell_price,
                'total_cost': total_cost,
                'revenue': revenue
            })

        return pd.DataFrame(profits)

    @st.cache_data(ttl=300)
    def get_craftable_item_ids():
        query = """
        SELECT DISTINCT ir.product_id
        FROM item_recipes ir
        WHERE EXISTS (
            SELECT 1
            FROM bazaar_prices bp
            WHERE bp.product_id = ir.product_id
              AND bp.snapshot_id = (SELECT id FROM snapshots ORDER BY collected_at DESC LIMIT 1)
        )
        ORDER BY ir.product_id;
        """
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                return [row[0] for row in cur.fetchall()]
        except Exception:
            return []

    def get_current_profits_all_items(force_refresh=False):
        if not force_refresh and "current_profits_df" in st.session_state:
            return st.session_state["current_profits_df"]

        item_ids = get_craftable_item_ids()
        if not item_ids:
            return pd.DataFrame()

        progress_bar = st.progress(0)
        status_text = st.empty()
        current_profits = []
        total_items = len(item_ids)

        for index, item_id in enumerate(item_ids, start=1):
            status_text.write(f"Calculating crafting profits: {index}/{total_items}")
            progress_bar.progress(index / total_items)

            profit_df = calculate_profit_over_time(item_id)
            if not profit_df.empty:
                latest = profit_df.iloc[-1]
                current_profits.append({
                    'item_id': item_id,
                    'displayname': get_item_data(item_id, fetch_icon=False)['displayname'],
                    'profit': latest['profit'],
                    'cost': latest['total_cost'],
                    'revenue': latest['revenue'],
                    'margin_pct': (latest['profit'] / max(latest['total_cost'], 1) * 100)
                })

        progress_bar.empty()
        status_text.empty()

        rankings_df = pd.DataFrame(current_profits)
        st.session_state["current_profits_df"] = rankings_df
        return rankings_df

    if st.button("Refresh Rankings"):
        st.session_state.pop("current_profits_df", None)
        st.rerun()

    rankings_df = get_current_profits_all_items()
    if not rankings_df.empty:
        rankings_df = rankings_df.sort_values('profit', ascending=False)

        display_df = rankings_df[['displayname', 'profit', 'cost', 'revenue', 'margin_pct']].copy()
        display_df.columns = ['Item', 'Profit/Craft', 'Total Cost', 'Revenue/Craft', 'Margin %']

        def profit_color(val):
            if val > 0:
                return 'color: #2ECC71'
            if val < 0:
                return 'color: #E74C3C'
            return ''

        styled_df = display_df.style.format({
            'Profit/Craft': '{:,.0f}',
            'Total Cost': '{:,.0f}',
            'Revenue/Craft': '{:,.0f}',
            'Margin %': '{:.1f}%'
        }).map(profit_color, subset=['Profit/Craft'])

        st.dataframe(styled_df, use_container_width=True)

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Profitable Items", len(rankings_df[rankings_df['profit'] > 0]))
        col2.metric("Unprofitable Items", len(rankings_df[rankings_df['profit'] <= 0]))
        col3.metric("Avg Profit", f"{rankings_df['profit'].mean():,.0f}")
        col4.metric("Avg Margin", f"{rankings_df['margin_pct'].mean():.1f}%")
    else:
        st.info("No craftable items found or no price data available.")
