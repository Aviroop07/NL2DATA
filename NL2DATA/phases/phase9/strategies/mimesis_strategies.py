"""Mimesis-based string and location generation strategies."""

from typing import List, Optional
from pydantic import Field, field_validator
from mimesis import Person, Address, Text, Datetime

from NL2DATA.phases.phase9.strategies.base import BaseGenerationStrategy


class MimesisNameStrategy(BaseGenerationStrategy):
    """Mimesis name generation strategy."""
    
    name: str = Field(default="mimesis_name", frozen=True)
    kind: str = Field(default="string", frozen=True)
    description: str = Field(
        default="Generate person names using Mimesis (international locale support). Better than Faker for non-English locales.",
        frozen=True
    )
    
    locale: str = Field(default="en", description="Locale code (e.g., 'en', 'ru', 'ja', 'zh')")
    name_type: str = Field(default="full", description="Type of name: 'full' (default), 'first', or 'last'")
    
    @field_validator("name_type")
    @classmethod
    def validate_name_type(cls, v: str) -> str:
        if v not in ["full", "first", "last"]:
            raise ValueError("name_type must be 'full', 'first', or 'last'")
        return v
    
    def generate(self, size: int) -> List[str]:
        """Generate Mimesis names."""
        person = Person(self.locale)
        if self.name_type == "first":
            return [person.first_name() for _ in range(size)]
        elif self.name_type == "last":
            return [person.last_name() for _ in range(size)]
        else:
            return [person.full_name() for _ in range(size)]


class MimesisEmailStrategy(BaseGenerationStrategy):
    """Mimesis email generation strategy."""
    
    name: str = Field(default="mimesis_email", frozen=True)
    kind: str = Field(default="string", frozen=True)
    description: str = Field(
        default="Generate email addresses using Mimesis (international locale support).",
        frozen=True
    )
    
    locale: str = Field(default="en", description="Locale code")
    domains: Optional[List[str]] = Field(default=None, description="Optional list of allowed domains (array of strings)")
    
    def generate(self, size: int) -> List[str]:
        """Generate Mimesis emails."""
        person = Person(self.locale)
        if self.domains:
            return [person.email(domains=self.domains) for _ in range(size)]
        return [person.email() for _ in range(size)]


class MimesisTextStrategy(BaseGenerationStrategy):
    """Mimesis text generation strategy."""
    
    name: str = Field(default="mimesis_text", frozen=True)
    kind: str = Field(default="string", frozen=True)
    description: str = Field(
        default="Generate text content using Mimesis (international locale support).",
        frozen=True
    )
    
    locale: str = Field(default="en", description="Locale code")
    text_type: str = Field(default="sentence", description="Type: 'word', 'sentence', or 'title'")
    
    @field_validator("text_type")
    @classmethod
    def validate_text_type(cls, v: str) -> str:
        if v not in ["word", "sentence", "title"]:
            raise ValueError("text_type must be 'word', 'sentence', or 'title'")
        return v
    
    def generate(self, size: int) -> List[str]:
        """Generate Mimesis text."""
        text = Text(self.locale)
        if self.text_type == "word":
            return [text.word() for _ in range(size)]
        elif self.text_type == "title":
            return [text.title() for _ in range(size)]
        else:
            return [text.sentence() for _ in range(size)]


class MimesisAddressStrategy(BaseGenerationStrategy):
    """Mimesis address generation strategy."""
    
    name: str = Field(default="mimesis_address", frozen=True)
    kind: str = Field(default="location", frozen=True)
    description: str = Field(
        default="Generate addresses using Mimesis (international locale support). Generates addresses matching the locale's address format.",
        frozen=True
    )
    
    locale: str = Field(default="en", description="Locale code")
    component: str = Field(default="full", description="Which component: 'full' (default), 'street', 'city', 'state', 'postal_code', 'country'")
    
    @field_validator("component")
    @classmethod
    def validate_component(cls, v: str) -> str:
        allowed = ["full", "street", "city", "state", "postal_code", "country"]
        if v not in allowed:
            raise ValueError(f"component must be one of {allowed}")
        return v
    
    def generate(self, size: int) -> List[str]:
        """Generate Mimesis addresses."""
        address = Address(self.locale)
        if self.component == "street":
            return [address.street_address() for _ in range(size)]
        elif self.component == "city":
            return [address.city() for _ in range(size)]
        elif self.component == "state":
            return [address.state() for _ in range(size)]
        elif self.component == "postal_code":
            return [address.postal_code() for _ in range(size)]
        elif self.component == "country":
            return [address.country() for _ in range(size)]
        else:
            return [address.address() for _ in range(size)]


class MimesisCoordinatesStrategy(BaseGenerationStrategy):
    """Mimesis coordinates generation strategy."""
    
    name: str = Field(default="mimesis_coordinates", frozen=True)
    kind: str = Field(default="location", frozen=True)
    description: str = Field(
        default="Generate geographic coordinates (latitude/longitude pairs) using Mimesis. Use for geographic coordinate attributes.",
        frozen=True
    )
    
    min_lat: Optional[float] = Field(default=None, description="Minimum latitude (default: -90)")
    max_lat: Optional[float] = Field(default=None, description="Maximum latitude (default: 90)")
    min_lon: Optional[float] = Field(default=None, description="Minimum longitude (default: -180)")
    max_lon: Optional[float] = Field(default=None, description="Maximum longitude (default: 180)")
    
    def generate(self, size: int) -> List[dict]:
        """Generate Mimesis coordinates."""
        address = Address()
        coords = []
        for _ in range(size):
            lat = address.latitude()
            lon = address.longitude()
            
            if self.min_lat is not None:
                lat = max(lat, self.min_lat)
            if self.max_lat is not None:
                lat = min(lat, self.max_lat)
            if self.min_lon is not None:
                lon = max(lon, self.min_lon)
            if self.max_lon is not None:
                lon = min(lon, self.max_lon)
            
            coords.append({"lat": lat, "lon": lon})
        return coords


class MimesisCountryStrategy(BaseGenerationStrategy):
    """Mimesis country generation strategy."""
    
    name: str = Field(default="mimesis_country", frozen=True)
    kind: str = Field(default="location", frozen=True)
    description: str = Field(
        default="Generate country names or codes using Mimesis. Use for country attributes.",
        frozen=True
    )
    
    locale: str = Field(default="en", description="Locale code")
    code_type: str = Field(default="name", description="Type: 'name' (country name), 'code'/'alpha2' (2-letter), 'alpha3' (3-letter)")
    
    @field_validator("code_type")
    @classmethod
    def validate_code_type(cls, v: str) -> str:
        if v not in ["name", "code", "alpha2", "alpha3"]:
            raise ValueError("code_type must be 'name', 'code', 'alpha2', or 'alpha3'")
        return v
    
    def generate(self, size: int) -> List[str]:
        """Generate Mimesis countries."""
        address = Address(self.locale)
        if self.code_type in ["code", "alpha2"]:
            return [address.country_code() for _ in range(size)]
        elif self.code_type == "alpha3":
            return [address.country_code(alpha_3=True) for _ in range(size)]
        else:
            return [address.country() for _ in range(size)]

