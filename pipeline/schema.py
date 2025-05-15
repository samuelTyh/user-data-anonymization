import logging
from dataclasses import dataclass
from typing import List

logger = logging.getLogger(__name__)


@dataclass
class FieldDefinition:
    """Definition of a database field with type and description."""
    name: str
    data_type: str
    description: str
    is_masked: bool = False
    
    def __str__(self) -> str:
        """Return the field definition as a SQL column definition string."""
        return f"{self.name} {self.data_type},    -- {self.description}"


@dataclass
class TableSchema:
    """Definition of a database table schema."""
    name: str
    fields: List[FieldDefinition]
    description: str
    
    def get_create_table_sql(self) -> str:
        """Generate the SQL CREATE TABLE statement for this schema."""
        field_definitions = [str(field) for field in self.fields]
        field_sql = "\n                ".join(field_definitions)
        
        return f"""
            CREATE TABLE IF NOT EXISTS {self.name} (
                {field_sql}
            )
        """
    
    def get_field_names(self) -> List[str]:
        """Get all field names in this schema."""
        return [field.name for field in self.fields]
    
    def get_masked_fields(self) -> List[str]:
        """Get names of fields marked as ****."""
        return [field.name for field in self.fields if field.is_masked]
    
    def get_non_masked_fields(self) -> List[str]:
        """Get names of fields not marked as ****."""
        return [field.name for field in self.fields if not field.is_masked]


@dataclass
class ViewDefinition:
    """Definition of a database view for reporting."""
    name: str
    query: str
    description: str
    
    def get_create_view_sql(self, table_name: str) -> str:
        """Generate the SQL CREATE VIEW statement for this view."""
        # Replace {table} placeholder with the actual table name
        view_query = self.query.replace("{table}", table_name)
        return f"CREATE OR REPLACE VIEW {self.name} AS {view_query}"


# Define the Person schema
PERSON_SCHEMA = TableSchema(
    name="persons",
    description="Anonymized person data from the Faker API",
    fields=[
        # Retained fields
        FieldDefinition("gender", "VARCHAR", "Male, Female or Other"),
        FieldDefinition("country", "VARCHAR", "Country name"),
        FieldDefinition("city", "VARCHAR", "City name"),
        FieldDefinition("country_code", "VARCHAR", "Country code (e.g., US)"),
        # Anonymized fields
        FieldDefinition("email", "VARCHAR", "Domain part only (anonymized)"),
        FieldDefinition("birthday", "VARCHAR", "Age group (e.g., [30-40])"),
        FieldDefinition("latitude", "FLOAT", "Loyalty sensitive hashing coordinate"),
        FieldDefinition("longitude", "FLOAT", "Loyalty sensitive hashing coordinate"),
        # PII fields (masked)
        FieldDefinition("firstname", "VARCHAR", "Masked (***)", is_masked=True),
        FieldDefinition("lastname", "VARCHAR", "Masked (***)", is_masked=True),
        FieldDefinition("phone", "VARCHAR", "Masked (***)", is_masked=True),
        FieldDefinition("street", "VARCHAR", "Masked (***)", is_masked=True),
        FieldDefinition("streetName", "VARCHAR", "Masked (***)", is_masked=True),
        FieldDefinition("buildingNumber", "VARCHAR", "Masked (***)", is_masked=True),
        FieldDefinition("zipcode", "VARCHAR", "Masked (***)", is_masked=True),
        FieldDefinition("image", "VARCHAR", "Masked (***)", is_masked=True),
        FieldDefinition("website", "VARCHAR", "Masked (***)", is_masked=True),
    ]
)

# Define reporting views
REPORTING_VIEWS = [
    ViewDefinition(
        name="email_provider_stats",
        description="Statistics on email provider usage",
        query="""
        SELECT 
            email AS email_provider,
            COUNT(*) AS user_count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {table}), 2) AS percentage
        FROM {table}
        GROUP BY email
        ORDER BY user_count DESC
        """
    ),
    ViewDefinition(
        name="country_stats",
        description="Country-based user distribution",
        query="""
        SELECT 
            country,
            COUNT(*) AS user_count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {table}), 2) AS percentage
        FROM {table}
        GROUP BY country
        ORDER BY user_count DESC
        """
    ),
    ViewDefinition(
        name="email_by_country",
        description="Email provider usage by country",
        query="""
        SELECT 
            country,
            email AS email_provider,
            COUNT(*) AS user_count,
            ROUND(COUNT(*) * 100.0 / (
                SELECT COUNT(*) FROM {table} p2 
                WHERE p2.country = p1.country
            ), 2) AS country_percentage
        FROM {table} p1
        GROUP BY country, email
        ORDER BY country, user_count DESC
        """
    ),
    ViewDefinition(
        name="age_group_stats",
        description="Age distribution across users",
        query="""
        SELECT 
            birthday AS age_group,
            COUNT(*) AS user_count,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {table}), 2) AS percentage
        FROM {table}
        GROUP BY birthday
        ORDER BY birthday
        """
    )
]
