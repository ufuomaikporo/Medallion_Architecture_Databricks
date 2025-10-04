import yaml
from pyspark.sql import SparkSession

# --------------------------
# Initialize Spark session
# --------------------------
spark = SparkSession.builder.getOrCreate()

# --------------------------
# Create catalog and schemas if they don't exist
# --------------------------

# Create catalog
spark.sql("CREATE CATALOG IF NOT EXISTS pipeline_catalog")

# Create schemas
for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS pipeline_catalog.{schema}")

print("✅ Catalog and schemas created.")

# --------------------------
# Load YAML file
# --------------------------
yaml_file_path = "pipeline_catalog_oil_data.yaml"
with open(yaml_file_path, 'r') as f:
    catalog_data = yaml.safe_load(f)

with open(yaml_file_path, 'r') as f:
    catalog_data = yaml.safe_load(f)

# Check for correct YAML structure
if not catalog_data or 'catalogs' not in catalog_data or catalog_data['catalogs'] is None:
    raise ValueError("YAML file is missing the 'catalogs' key or is empty.")

# --------------------------
# Helper function to convert dict to SQL tag string
# --------------------------

def dict_to_tag_sql(tag_dict):
    """
    Converts a dictionary of tags to a SQL SET TAGS string
    """
    tag_list = []
    for k, v in tag_dict.items():
        tag_list.append(f'"{k}" = \'{v}\'')
    return ",\n  ".join(tag_list)

# --------------------------
# Apply tags to catalog, schemas, and tables
# --------------------------
for catalog_name, catalog_info in catalog_data['catalogs'].items():
    # --------------------------
    # Catalog-level tags
    # --------------------------
    catalog_tags = {k: v for k, v in catalog_info.items() if k != "schemas"}
    tag_sql = dict_to_tag_sql(catalog_tags)
    sql_stmt = f"""
    ALTER CATALOG {catalog_name}
    SET TAGS (
      {tag_sql}
    )
    """
    print(f"Applying tags to catalog '{catalog_name}'...")
    spark.sql(sql_stmt)

    # --------------------------
    # Schema-level tags
    # --------------------------
    if "schemas" in catalog_info:
        for schema_name, schema_tags in catalog_info["schemas"].items():
            spark.sql(f"USE CATALOG {catalog_name}")
            tag_sql = dict_to_tag_sql(schema_tags)
            sql_stmt = f"""
            ALTER SCHEMA {catalog_name}.{schema_name}
            SET TAGS (
            {tag_sql}
            )
            """
            print(f"Applying tags to schema '{catalog_name}.{schema_name}'...")
            try:
                spark.sql(sql_stmt)
            except Exception as e:
                print(f"⚠️ Could not apply tags to schema '{catalog_name}.{schema_name}': {e}")
                continue

            # Table-level tags
            try:
                tables = [t.tableName for t in spark.catalog.listTables(schema_name, catalog_name)]
            except Exception as e:
                print(f"⚠️ Could not list tables in schema '{catalog_name}.{schema_name}': {e}")
                continue

            for table_name in tables:
                sql_stmt = f"""
                ALTER TABLE {catalog_name}.{schema_name}.{table_name}
                SET TAGS (
                {tag_sql}
                )
                """
                print(f"Applying tags to table '{catalog_name}.{schema_name}.{table_name}'...")
                try:
                    spark.sql(sql_stmt)
                except Exception as e:
                    print(f"⚠️ Could not apply tags to table '{catalog_name}.{schema_name}.{table_name}': {e}")

print("✅ All catalog, schema, and table tags applied successfully.")
