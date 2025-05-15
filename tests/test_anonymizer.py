from datetime import datetime
from dateutil.relativedelta import relativedelta

from pipeline.anonymizer import DataAnonymizer


class TestDataAnonymizer:
    """Tests for the DataAnonymizer class."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.anonymizer = DataAnonymizer()
        
        # Sample person data for testing
        self.sample_person = {
            "id": 1,
            "firstname": "Esther",
            "lastname": "Connelly",
            "email": "gordon91@hotmail.com",
            "phone": "+14146859592",
            "birthday": "2024-01-12",
            "gender": "female",
            "address": {
                "id": 1,
                "street": "4775 Eduardo Ferry",
                "streetName": "Konopelski Trail",
                "buildingNumber": "436",
                "city": "Myrnaland",
                "zipcode": "46965",
                "country": "Uganda",
                "country_code": "UG",
                "latitude": -59.697831,
                "longitude": -121.69404
            },
            "website": "http://hodkiewicz.net",
            "image": "http://placeimg.com/640/480/people"
        }
    
    def test_anonymize_person(self):
        """Test person anonymization."""
        # Anonymize the sample person
        anonymized = self.anonymizer._anonymize_person(self.sample_person)
        
        # Check retained fields
        assert anonymized["country"] == "Uganda"
        assert anonymized["city"] == "Myrnaland"
        assert anonymized["email"] == "hotmail.com"
        assert anonymized["birthday"].startswith("[")
        assert anonymized["birthday"].endswith("]")
        assert anonymized["gender"] == "female"
        
        # Check masked PII fields
        assert anonymized["firstname"] == "****"
        assert anonymized["lastname"] == "****"
        assert anonymized["phone"] == "****"
        assert anonymized["latitude"] != -59.697831
        assert anonymized["longitude"] != -121.69404
        assert anonymized["street"] == "****"
        assert anonymized["streetName"] == "****"
        assert anonymized["buildingNumber"] == "****"
        assert anonymized["zipcode"] == "****"
    
    def test_anonymize_email(self):
        """Test email anonymization."""
        # Test valid email
        assert self.anonymizer._anonymize_email("user@example.com") == "example.com"
        
        # Test email with multiple @ symbols
        assert self.anonymizer._anonymize_email("user@sub@example.com") == "****@****"
        
        # Test invalid email
        assert self.anonymizer._anonymize_email("invalid-email") == "****@****"
        
        # Test empty email
        assert self.anonymizer._anonymize_email("") == "****@****"
    
    def test_generalize_age(self):
        """Test age generalization."""
        # Get current date for age calculation
        today = datetime.now()
        
        # Test exact decade boundary (30 years old)
        thirty_years_ago = (today - relativedelta(years=30)).strftime("%Y-%m-%d")
        assert self.anonymizer._generalize_age(thirty_years_ago) == "[30-40]"
        
        # Test middle of decade (35 years old)
        thirty_five_years_ago = (today - relativedelta(years=35)).strftime("%Y-%m-%d")
        assert self.anonymizer._generalize_age(thirty_five_years_ago) == "[30-40]"
        
        # Test senior age (65 years old)
        sixty_five_years_ago = (today - relativedelta(years=65)).strftime("%Y-%m-%d")
        assert self.anonymizer._generalize_age(sixty_five_years_ago) == "[60-70]"
        
        # Test invalid date format
        assert self.anonymizer._generalize_age("invalid-date") == "[unknown]"
    
    def test_anonymize_persons(self):
        """Test batch anonymization of multiple persons."""
        # Create a list of sample persons
        persons = [self.sample_person] * 3
        
        # Anonymize the list
        anonymized_persons = self.anonymizer.anonymize_persons(persons)
        
        # Check the result
        assert len(anonymized_persons) == 3
        for person in anonymized_persons:
            assert person["email"] == "hotmail.com"
            assert person["country"] == "Uganda"
            assert person["firstname"] == "****"
