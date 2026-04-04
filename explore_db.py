"""
Quick script to explore PostgreSQL database structure and relationships
"""
import os
from sqlalchemy import text
from dotenv import load_dotenv
from database.db_config import engine as db_engine

load_dotenv()

def explore_tables():
    """Show table names and their structures"""
    with db_engine.connect() as conn:
        # List tables
        print("="*60)
        print("TABLES IN DATABASE")
        print("="*60)
        tables_query = text("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        tables = conn.execute(tables_query).fetchall()
        for table in tables:
            print(f"  - {table[0]}")
        
        # Explore content table
        print("\n" + "="*60)
        print("CONTENT TABLE STRUCTURE")
        print("="*60)
        content_cols = text("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'content' 
            ORDER BY ordinal_position
        """)
        cols = conn.execute(content_cols).fetchall()
        for col in cols:
            print(f"  {col[0]:.<30} {col[1]}")
        
        # Explore expert_user table
        print("\n" + "="*60)
        print("EXPERT_USER TABLE STRUCTURE")
        print("="*60)
        user_cols = text("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'expert_user' 
            ORDER BY ordinal_position
        """)
        cols = conn.execute(user_cols).fetchall()
        for col in cols:
            print(f"  {col[0]:.<30} {col[1]}")
        
        # Explore md_app_languages table
        print("\n" + "="*60)
        print("MD_APP_LANGUAGES TABLE STRUCTURE")
        print("="*60)
        lang_cols = text("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'md_app_languages' 
            ORDER BY ordinal_position
        """)
        cols = conn.execute(lang_cols).fetchall()
        for col in cols:
            print(f"  {col[0]:.<30} {col[1]}")


def explore_data():
    """Show sample data and relationships"""
    with db_engine.connect() as conn:
        # Sample content
        print("\n" + "="*60)
        print("SAMPLE CONTENT (first 5 records)")
        print("="*60)
        content_query = text("""
            SELECT id, title, created_by, language_id, status 
            FROM public.content 
            LIMIT 5
        """)
        rows = conn.execute(content_query).fetchall()
        for row in rows:
            print(f"  ID={row[0]}, Title={row[1][:40]}, Created_by={row[2]}, Language_id={row[3]}, Status={row[4]}")
        
        # Sample expert_user
        print("\n" + "="*60)
        print("SAMPLE EXPERT_USER (first 5 records)")
        print("="*60)
        user_query = text("""
            SELECT user_id, account_id, status 
            FROM public.expert_user 
            LIMIT 5
        """)
        rows = conn.execute(user_query).fetchall()
        for row in rows:
            print(f"  User_id={row[0]}, Account_id={row[1]}, Status={row[2]}")
        
        # sample md_app_languages
        print("\n" + "="*60)
        print("SAMPLE MD_APP_LANGUAGES")
        print("="*60)
        lang_query = text("""
            SELECT id, language_code, language_name 
            FROM public.md_app_languages 
            LIMIT 10
        """)
        rows = conn.execute(lang_query).fetchall()
        for row in rows:
            print(f"  ID={row[0]}, Code={row[1]}, Name={row[2]}")
        
        # Count content by account_id
        print("\n" + "="*60)
        print("CONTENT COUNT BY CREATOR ACCOUNT_ID (top 10)")
        print("="*60)
        count_query = text("""
            SELECT 
                eu.account_id,
                COUNT(c.id) as content_count
            FROM public.content c
            INNER JOIN public.expert_user eu
                ON eu.user_id = c.created_by
            WHERE c.status = 1 AND eu.status = 1
            GROUP BY eu.account_id
            ORDER BY content_count DESC
            LIMIT 10
        """)
        rows = conn.execute(count_query).fetchall()
        for row in rows:
            print(f"  Account_ID={row[0]:.<10} Content_Count={row[1]}")
        
        # Content for account_id=14
        print("\n" + "="*60)
        print("CONTENT FOR ACCOUNT_ID=14")
        print("="*60)
        acct_query = text("""
            SELECT 
                c.id,
                c.title,
                c.created_by,
                eu.account_id,
                COALESCE(ml.language_code, 'en') as language_code,
                c.status
            FROM public.content c
            INNER JOIN public.expert_user eu
                ON eu.user_id = c.created_by
            LEFT JOIN public.md_app_languages ml
                ON ml.id = c.language_id
            WHERE eu.account_id = 14 AND c.status = 1
            ORDER BY c.id
        """)
        rows = conn.execute(acct_query).fetchall()
        print(f"Total records for account_id=14: {len(rows)}\n")
        for row in rows[:10]:  # Show first 10
            print(f"  ID={row[0]:<5} Title={row[1][:40]:<40} Creator={row[2]:<5} Account={row[3]:<5} Lang={row[4]:<5} Status={row[5]}")
        if len(rows) > 10:
            print(f"  ... and {len(rows) - 10} more")


if __name__ == "__main__":
    print("\n" + "#"*60)
    print("# POSTGRESQL DATABASE EXPLORATION")
    print("#"*60 + "\n")
    
    explore_tables()
    explore_data()
    
    print("\n" + "#"*60)
    print("# END OF EXPLORATION")
    print("#"*60 + "\n")
