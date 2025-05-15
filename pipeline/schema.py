import logging
from dataclasses import dataclass, field
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
        FieldDefinition("firstname", "VARCHAR", "Masked (****)", is_masked=True),
        FieldDefinition("lastname", "VARCHAR", "Masked (****)", is_masked=True),
        FieldDefinition("phone", "VARCHAR", "Masked (****)", is_masked=True),
        FieldDefinition("street", "VARCHAR", "Masked (****)", is_masked=True),
        FieldDefinition("streetName", "VARCHAR", "Masked (****)", is_masked=True),
        FieldDefinition("buildingNumber", "VARCHAR", "Masked (****)", is_masked=True),
        FieldDefinition("zipcode", "VARCHAR", "Masked (****)", is_masked=True),
        FieldDefinition("image", "VARCHAR", "Masked (****)", is_masked=True),
        FieldDefinition("website", "VARCHAR", "Masked (****)", is_masked=True),
    ]
)
