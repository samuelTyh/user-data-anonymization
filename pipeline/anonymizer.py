import logging
from datetime import datetime
import random
from typing import Dict, List, Any

from .schema import PERSON_SCHEMA

logger = logging.getLogger(__name__)


class DataAnonymizer:
    """
    Anonymizes personal data according to privacy guidelines.
    
    Implements:
    - Data masking for PII (Personally Identifiable Information)
    - Data generalization for specific fields (birthday, email)
    - Retention of only required fields (location, age, email provider)
    """
    
    def __init__(self):
        """Initialize the data anonymizer."""
        # Get field definitions from schema
        self.pii_fields = PERSON_SCHEMA.get_masked_fields()
        
        # Define fields to retain and their anonymization needs
        self.retained_fields = {
            "gender": self._pass_through,
            "country": self._pass_through,
            "city": self._pass_through,
            "country_code": self._pass_through,
            "email": self._anonymize_email,           # Email provider
            "birthday": self._generalize_age,         # Age group
            "latitude": self._anonymize_coordinate,   # Anonymized latitude
            "longitude": self._anonymize_coordinate,  # Anonymized longitude
        }
    
    def anonymize_persons(self, persons: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Anonymize a list of person records.
        
        Args:
            persons: List of person dictionaries from the API
            
        Returns:
            List of anonymized person dictionaries
        """
        anonymized_persons = []
        
        for person in persons:
            try:
                anonymized_person = self._anonymize_person(person)
                anonymized_persons.append(anonymized_person)
            except Exception as e:
                logger.error(f"Error anonymizing person: {str(e)}")
                # Skip records that can't be properly anonymized
                continue
        
        logger.info(f"Anonymized {len(anonymized_persons)} person records")
        return anonymized_persons
    
    def _anonymize_person(self, person: Dict[str, Any]) -> Dict[str, Any]:
        """
        Anonymize a single person record.
        
        Args:
            person: Original person dictionary
            
        Returns:
            Anonymized person dictionary
        """
        # Start with an empty dict and add only retained fields
        anonymized = {}
        
        # Apply appropriate anonymization to each retained field
        for field, anonymize_func in self.retained_fields.items():
            if field in person:
                anonymized[field] = anonymize_func(person[field])
            # Handle fields for address object
            elif field in person.get('address', {}):
                anonymized[field] = anonymize_func(person.get('address')[field])
            else:
                logger.warning(f"Field '{field}' not found in person data")
        
        # Add masked versions of PII fields (for completeness, if needed)
        for field in self.pii_fields:
            if field in person:
                non_anonymized_value = person[field]
                logger.debug(f"Masking PII field '{field}' with value '{non_anonymized_value}'")
                # Mask the PII field
                anonymized[field] = "****"
            elif field in person.get('address', {}):
                non_anonymized_value = person.get('address')[field]
                logger.debug(f"Masking PII field '{field}' with value '{non_anonymized_value}'")
                # Mask the PII field
                anonymized[field] = "****"
            else:
                logger.warning(f"PII field '{field}' not found in person data")
        
        return anonymized
    
    def _pass_through(self, value: Any) -> Any:
        """
        Pass through the value unchanged (for non-PII fields).
        
        Args:
            value: Original field value
            
        Returns:
            Unchanged value
        """
        return value
    
    def _anonymize_email(self, email: str) -> str:
        """
        Anonymize email by keeping only the domain part.
        
        Args:
            email: Original email (e.g., user@example.com)
            
        Returns:
            Keep domain part only (e.g., example.com)
        """
        try:
            parts = email.split('@')
            if len(parts) != 2:
                logger.warning(f"Invalid email format: {email}")
                return "****@****"
            # Return the domain part of the email
            return parts[1]
        except Exception as e:
            logger.error(f"Error anonymizing email: {str(e)}")
            return "****@****"
    
    def _generalize_age(self, birthday: str) -> str:
        """
        Generalize birthday to a 10-year age group.
        
        Args:
            birthday: Original birthday in YYYY-MM-DD format
            
        Returns:
            Age group string (e.g., [30-40])
        """
        try:
            # Parse the birthday
            birth_date = datetime.strptime(birthday, "%Y-%m-%d")
            
            # Calculate age
            today = datetime.now()
            age = today.year - birth_date.year
            
            # Adjust age if birthday hasn't occurred yet this year
            if (today.month, today.day) < (birth_date.month, birth_date.day):
                age -= 1
            
            # Calculate age group (floor to nearest 10)
            age_group_start = (age // 10) * 10
            age_group_end = age_group_start + 10
            
            return f"[{age_group_start}-{age_group_end}]"
        except Exception as e:
            logger.error(f"Error generalizing age: {str(e)}")
            return "[unknown]"

    def _anonymize_coordinate(self, coordinate: float, radius_km: int = 10):
        """
        Anonymize coordinates by adding random noise within a specified radius.
        
        Args:
            coordinates: 'latitude' or 'longitude' value
            radius_km: Radius in kilometers for noise addition
        Returns:
            anonymized latitude or longitude
        """
        # Convert radius to degrees (approximation at the equator)
        radius_degrees = radius_km / 111.139  # 111.139 km per degree
        if coordinate is None:
            logger.warning("Coordinate missing for anonymization")
            return None
        # Generate random noise based on the hash of the coordinates
        logger.debug(f"Anonymizing coordinate: {coordinate} with radius {radius_km} km")
        anon = hash(coordinate) % radius_km
        coordinate = coordinate + anon * random.choice([-1, 1]) * radius_degrees
        logger.debug(f"Anonymized coordinate: {coordinate}")
        # Round to 6 decimal places for precision
        return round(coordinate, 6)
