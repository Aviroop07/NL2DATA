"""Faker-based string generation strategies."""

from typing import List, Optional
from pydantic import Field, field_validator
from faker import Faker

from NL2DATA.phases.phase7.strategies.base import BaseGenerationStrategy


class FakerNameStrategy(BaseGenerationStrategy):
    """Faker name generation strategy."""
    
    name: str = Field(default="faker_name", frozen=True)
    kind: str = Field(default="string", frozen=True)
    description: str = Field(
        default="Generate person names using Faker. Use for person name attributes (e.g., customer_name, employee_name, author_name).",
        frozen=True
    )
    
    locale: str = Field(default="en_US", description="Locale for name generation (e.g., 'en_US', 'es_ES')")
    name_type: str = Field(default="full", description="Type of name: 'full' (default), 'first', or 'last'")
    
    @field_validator("name_type")
    @classmethod
    def validate_name_type(cls, v: str) -> str:
        if v not in ["full", "first", "last"]:
            raise ValueError("name_type must be 'full', 'first', or 'last'")
        return v
    
    def generate(self, size: int) -> List[str]:
        """Generate Faker names."""
        fake = Faker(self.locale)
        if self.name_type == "first":
            return [fake.first_name() for _ in range(size)]
        elif self.name_type == "last":
            return [fake.last_name() for _ in range(size)]
        else:
            return [fake.name() for _ in range(size)]


class FakerEmailStrategy(BaseGenerationStrategy):
    """Faker email generation strategy."""
    
    name: str = Field(default="faker_email", frozen=True)
    kind: str = Field(default="string", frozen=True)
    description: str = Field(
        default="Generate email addresses using Faker. Use for email address attributes.",
        frozen=True
    )
    
    locale: str = Field(default="en_US", description="Locale for email generation")
    domain: Optional[str] = Field(default=None, description="Optional fixed domain (e.g., 'example.com')")
    
    def generate(self, size: int) -> List[str]:
        """Generate Faker emails."""
        fake = Faker(self.locale)
        if self.domain:
            return [fake.email(domain=self.domain) for _ in range(size)]
        return [fake.email() for _ in range(size)]


class FakerAddressStrategy(BaseGenerationStrategy):
    """Faker address generation strategy."""
    
    name: str = Field(default="faker_address", frozen=True)
    kind: str = Field(default="string", frozen=True)
    description: str = Field(
        default="Generate addresses using Faker. Use for address-related attributes (e.g., street_address, city, state, zipcode).",
        frozen=True
    )
    
    locale: str = Field(default="en_US", description="Locale for address generation")
    component: str = Field(default="full", description="Which component: 'full' (default), 'street', 'city', 'state', 'zipcode', 'country'")
    
    @field_validator("component")
    @classmethod
    def validate_component(cls, v: str) -> str:
        allowed = ["full", "street", "city", "state", "zipcode", "country"]
        if v not in allowed:
            raise ValueError(f"component must be one of {allowed}")
        return v
    
    def generate(self, size: int) -> List[str]:
        """Generate Faker addresses."""
        fake = Faker(self.locale)
        if self.component == "street":
            return [fake.street_address() for _ in range(size)]
        elif self.component == "city":
            return [fake.city() for _ in range(size)]
        elif self.component == "state":
            return [fake.state() for _ in range(size)]
        elif self.component == "zipcode":
            return [fake.zipcode() for _ in range(size)]
        elif self.component == "country":
            return [fake.country() for _ in range(size)]
        else:
            return [fake.address() for _ in range(size)]


class FakerCompanyStrategy(BaseGenerationStrategy):
    """Faker company name generation strategy."""
    
    name: str = Field(default="faker_company", frozen=True)
    kind: str = Field(default="string", frozen=True)
    description: str = Field(
        default="Generate company names using Faker. Use for company/business name attributes.",
        frozen=True
    )
    
    locale: str = Field(default="en_US", description="Locale for company name generation")
    
    def generate(self, size: int) -> List[str]:
        """Generate Faker company names."""
        fake = Faker(self.locale)
        return [fake.company() for _ in range(size)]


class FakerTextStrategy(BaseGenerationStrategy):
    """Faker text generation strategy."""
    
    name: str = Field(default="faker_text", frozen=True)
    kind: str = Field(default="string", frozen=True)
    description: str = Field(
        default="Generate random text using Faker. Use for free-text attributes (e.g., description, comment, notes, bio).",
        frozen=True
    )
    
    locale: str = Field(default="en_US", description="Locale for text generation")
    text_type: str = Field(default="sentence", description="Type: 'word', 'sentence', 'paragraph', or 'text' (multiple paragraphs)")
    max_nb_chars: Optional[int] = Field(default=None, description="Maximum characters (for paragraph/text)")
    
    @field_validator("text_type")
    @classmethod
    def validate_text_type(cls, v: str) -> str:
        if v not in ["word", "sentence", "paragraph", "text"]:
            raise ValueError("text_type must be 'word', 'sentence', 'paragraph', or 'text'")
        return v
    
    def generate(self, size: int) -> List[str]:
        """Generate Faker text."""
        fake = Faker(self.locale)
        if self.text_type == "word":
            return [fake.word() for _ in range(size)]
        elif self.text_type == "sentence":
            return [fake.sentence() for _ in range(size)]
        elif self.text_type == "paragraph":
            if self.max_nb_chars:
                return [fake.paragraph(max_nb_chars=self.max_nb_chars) for _ in range(size)]
            return [fake.paragraph() for _ in range(size)]
        else:  # text
            if self.max_nb_chars:
                return [fake.text(max_nb_chars=self.max_nb_chars) for _ in range(size)]
            return [fake.text() for _ in range(size)]


class FakerURLStrategy(BaseGenerationStrategy):
    """Faker URL generation strategy."""
    
    name: str = Field(default="faker_url", frozen=True)
    kind: str = Field(default="string", frozen=True)
    description: str = Field(
        default="Generate URLs using Faker. Use for URL/URI attributes.",
        frozen=True
    )
    
    locale: str = Field(default="en_US", description="Locale for URL generation")
    url_type: str = Field(default="url", description="Type: 'url' (full URL), 'domain' (domain only), 'uri' (path only)")
    
    @field_validator("url_type")
    @classmethod
    def validate_url_type(cls, v: str) -> str:
        if v not in ["url", "domain", "uri"]:
            raise ValueError("url_type must be 'url', 'domain', or 'uri'")
        return v
    
    def generate(self, size: int) -> List[str]:
        """Generate Faker URLs."""
        fake = Faker(self.locale)
        if self.url_type == "domain":
            return [fake.domain_name() for _ in range(size)]
        elif self.url_type == "uri":
            return [fake.uri_path() for _ in range(size)]
        else:
            return [fake.url() for _ in range(size)]


class FakerPhoneStrategy(BaseGenerationStrategy):
    """Faker phone number generation strategy."""
    
    name: str = Field(default="faker_phone", frozen=True)
    kind: str = Field(default="string", frozen=True)
    description: str = Field(
        default="Generate phone numbers using Faker. Use for phone number attributes.",
        frozen=True
    )
    
    locale: str = Field(default="en_US", description="Locale for phone number generation")
    
    def generate(self, size: int) -> List[str]:
        """Generate Faker phone numbers."""
        fake = Faker(self.locale)
        return [fake.phone_number() for _ in range(size)]

