"""
Check database schema for user and expert_user tables
"""

from sqlalchemy import text, inspect
from database.db_config import engine as db_engine


def check_table_schema(table_name: str):
    """Inspect table schema"""
    print(f"\n📋 Table: {table_name}")
    print("="*60)
    
    query = text(f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = '{table_name}'
        ORDER BY ordinal_position
    """)
    
    try:
        with db_engine.connect() as conn:
            results = conn.execute(query).mappings().all()
        
        print(f"Columns:")
        for row in results:
            nullable = "nullable" if row.get('is_nullable') == 'YES' else "NOT NULL"
            print(f"  • {row.get('column_name')}: {row.get('data_type')} ({nullable})")
        
        return [row.get('column_name') for row in results]
    except Exception as e:
        print(f"❌ Error: {e}")
        return []


def main():
    print("\n" + "="*60)
    print("DATABASE SCHEMA INSPECTION")
    print("="*60)
    
    # Check user table
    check_table_schema("user")
    
    # Check expert_user table
    check_table_schema("expert_user")
    
    # Check available indicator tables
    query = text("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name LIKE '%indicator%'
        ORDER BY table_name
    """)
    
    print(f"\n📋 Tables containing 'indicator':")
    print("="*60)
    
    try:
        with db_engine.connect() as conn:
            tables = conn.execute(query).scalars().all()
        
        for table in tables:
            print(f"\n  • {table}")
            check_table_schema(table)
    except Exception as e:
        print(f"❌ Error: {e}")


if __name__ == "__main__":
    main()
