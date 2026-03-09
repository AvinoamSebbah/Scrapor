import os
import psycopg2

db_url = os.getenv('SUPABASE_DATABASE_URL')

def update_schema():
    conn = psycopg2.connect(db_url)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS prices CASCADE;")
        cur.execute("DROP TABLE IF EXISTS promotions CASCADE;")
        
        cur.execute("""
        CREATE TABLE prices (
            id SERIAL PRIMARY KEY,
            chain_id VARCHAR NOT NULL,
            item_code VARCHAR NOT NULL,
            base_price VARCHAR,
            store_prices JSONB DEFAULT '{}'::jsonb,
            available_in_store_ids TEXT[] DEFAULT '{}',
            item_type VARCHAR,
            unit_qty VARCHAR,
            quantity VARCHAR,
            unit_of_measure VARCHAR,
            b_is_weighted BOOLEAN DEFAULT false,
            qty_in_package VARCHAR,
            price_update_date TIMESTAMP,
            allow_discount VARCHAR,
            item_status VARCHAR,
            item_id VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(chain_id, item_code)
        );
        """)
        
        cur.execute("""
        CREATE INDEX idx_prices_chain_id ON prices(chain_id);
        CREATE INDEX idx_prices_item_code ON prices(item_code);
        """)
        
        cur.execute("""
        CREATE TABLE promotions (
            id SERIAL PRIMARY KEY,
            chain_id VARCHAR NOT NULL,
            promotion_id VARCHAR NOT NULL,
            sub_chain_id VARCHAR,
            bikoret_no VARCHAR,
            promotion_description VARCHAR,
            promotion_update_date TIMESTAMP,
            promotion_start_date DATE,
            promotion_start_hour VARCHAR,
            promotion_end_date DATE,
            promotion_end_hour VARCHAR,
            promotion_days VARCHAR,
            redemption_limit VARCHAR,
            reward_type VARCHAR,
            allow_multiple_discounts VARCHAR,
            is_weighted_promo BOOLEAN DEFAULT false,
            is_gift_item VARCHAR,
            min_no_of_item_offered VARCHAR,
            additional_is_coupon VARCHAR,
            additional_gift_count VARCHAR,
            additional_is_total VARCHAR,
            additional_is_active VARCHAR,
            additional_restrictions VARCHAR,
            remarks VARCHAR,
            min_qty VARCHAR,
            discounted_price VARCHAR,
            discounted_price_per_mida VARCHAR,
            weight_unit VARCHAR,
            club_id VARCHAR,
            items JSONB,
            store_promotions JSONB DEFAULT '{}'::jsonb,
            available_in_store_ids TEXT[] DEFAULT '{}',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(chain_id, promotion_id)
        );
        """)
        
        cur.execute("""
        CREATE INDEX idx_promotions_chain_id ON promotions(chain_id);
        CREATE INDEX idx_promotions_promotion_id ON promotions(promotion_id);
        """)
        
        cur.execute("TRUNCATE TABLE stores, products, processed_files CASCADE;")
        print("Schema updated with ALL promotion fields, tables prices & promos recreated, other tables truncated.")

if __name__ == '__main__':
    update_schema()
