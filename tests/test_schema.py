from pipeline.schema import FieldDefinition, TableSchema, PERSON_SCHEMA

class TestSchema:
    """Tests for the schema module."""
    
    def test_field_definition(self):
        """Test field definition class."""
        field = FieldDefinition(
            name="test_field",
            data_type="VARCHAR",
            description="Test description",
            is_masked=True
        )
        
        # Test string representation
        assert str(field) == "test_field VARCHAR,    -- Test description"
        
        # Test attributes
        assert field.name == "test_field"
        assert field.data_type == "VARCHAR"
        assert field.description == "Test description"
        assert field.is_masked is True
    
    def test_table_schema(self):
        """Test table schema class."""
        # Create a simple test schema
        fields = [
            FieldDefinition("id", "INTEGER", "Primary key"),
            FieldDefinition("name", "VARCHAR", "User name", is_masked=True),
            FieldDefinition("age", "INTEGER", "User age")
        ]
        
        schema = TableSchema(
            name="test_table",
            description="Test table description",
            fields=fields
        )
        
        # Test get_field_names method
        assert schema.get_field_names() == ["id", "name", "age"]
        
        # Test get_masked_fields method
        assert schema.get_masked_fields() == ["name"]
        
        # Test get_non_masked_fields method
        assert schema.get_non_masked_fields() == ["id", "age"]
        
        # Test get_create_table_sql method
        sql = schema.get_create_table_sql()
        assert "CREATE TABLE IF NOT EXISTS test_table" in sql
        assert "id INTEGER" in sql
        assert "name VARCHAR" in sql
        assert "age INTEGER" in sql
    
    def test_person_schema(self):
        """Test the predefined PERSON_SCHEMA."""
        # Test schema name
        assert PERSON_SCHEMA.name == "persons"
        
        # Test field counts
        assert len(PERSON_SCHEMA.fields) > 0
        
        # Test PII field detection
        pii_fields = PERSON_SCHEMA.get_masked_fields()
        non_pii_fields = PERSON_SCHEMA.get_non_masked_fields()
        
        # These should be in the PII fields list
        assert "firstname" in pii_fields
        assert "lastname" in pii_fields
        assert "phone" in pii_fields
        
        # These should be in the non-PII fields list
        assert "gender" in non_pii_fields
        assert "country" in non_pii_fields
        assert "email" in non_pii_fields
        
        # Check that each field is only in one list
        for field in pii_fields:
            assert field not in non_pii_fields
            
        for field in non_pii_fields:
            assert field not in pii_fields
        
        # Test SQL generation
        sql = PERSON_SCHEMA.get_create_table_sql()
        assert "CREATE TABLE IF NOT EXISTS persons" in sql
