"""
Check the actual schema of content table to fix video recommendation
"""

from sqlalchemy import text, inspect
from database.db_config import engine as db_engine

def check_schema():
    """Check content table schema"""
    
    print("\n" + "="*80)
    print("DATABASE SCHEMA INSPECTION")
    print("="*80)
    
    # Check content table columns
    print("\n📋 Content Table Columns:")
    query = text(
        """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'content'
        ORDER BY ordinal_position
        """
    )
    
    try:
        with db_engine.connect() as conn:
            results = conn.execute(query).mappings().all()
        
        for row in results:
            print(f"  • {row['column_name']:30s} | {row['data_type']:20s} | Nullable: {row['is_nullable']}")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Check expert_user table
    print("\n📋 Expert User Table Columns:")
    query = text(
        """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'expert_user'
        ORDER BY ordinal_position
        """
    )
    
    try:
        with db_engine.connect() as conn:
            results = conn.execute(query).mappings().all()
        
        for row in results:
            print(f"  • {row['column_name']:30s} | {row['data_type']:20s} | Nullable: {row['is_nullable']}")
    except Exception as e:
        print(f"❌ Error: {e}")
    
    # Get sample content record
    print("\n📋 Sample Content Record:")
    query = text("SELECT * FROM public.content LIMIT 1")
    
    try:
        with db_engine.connect() as conn:
            result = conn.execute(query).mappings().first()
        
        if result:
            print("\nColumns and values:")
            for key, val in dict(result).items():
                val_str = str(val)[:50] if val else "NULL"
                print(f"  • {key:30s} = {val_str}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    check_schema()
