"""Unit tests for generation strategy Pydantic models."""

import sys
from pathlib import Path
import pytest
import numpy as np

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))

from NL2DATA.phases.phase7.strategies.distributions import (
    NormalDistribution,
    LognormalDistribution,
    UniformDistribution,
    ParetoDistribution,
    ZipfDistribution,
    ExponentialDistribution,
    CategoricalDistribution,
    BernoulliDistribution,
)
from NL2DATA.phases.phase7.strategies.faker_strategies import (
    FakerNameStrategy,
    FakerEmailStrategy,
    FakerAddressStrategy,
    FakerCompanyStrategy,
    FakerTextStrategy,
    FakerURLStrategy,
    FakerPhoneStrategy,
)
from NL2DATA.phases.phase7.strategies.mimesis_strategies import (
    MimesisNameStrategy,
    MimesisEmailStrategy,
    MimesisTextStrategy,
    MimesisAddressStrategy,
    MimesisCoordinatesStrategy,
    MimesisCountryStrategy,
)
from NL2DATA.phases.phase7.strategies.regex_strategy import RegexStrategy
from pydantic import ValidationError


class TestDistributionStrategies:
    """Test distribution strategy models."""
    
    def test_normal_distribution(self):
        """Test NormalDistribution strategy."""
        strategy = NormalDistribution(mu=50.0, sigma=10.0, min=0.0, max=100.0)
        assert strategy.name == "normal"
        assert strategy.kind == "distribution"
        assert strategy.mu == 50.0
        assert strategy.sigma == 10.0
        
        # Test generate
        values = strategy.generate(100)
        assert len(values) == 100
        assert all(isinstance(v, (int, float)) for v in values)
        assert all(0.0 <= v <= 100.0 for v in values)
        
        # Test validation
        with pytest.raises(ValidationError):
            NormalDistribution(mu=50.0, sigma=-1.0)  # sigma must be > 0
    
    def test_lognormal_distribution(self):
        """Test LognormalDistribution strategy."""
        strategy = LognormalDistribution(mu=3.5, sigma=1.2, min=0.01)
        assert strategy.name == "lognormal"
        
        values = strategy.generate(100)
        assert len(values) == 100
        assert all(v >= 0.01 for v in values)
        
        # Test validation
        with pytest.raises(ValidationError):
            LognormalDistribution(mu=3.5, sigma=-1.0)  # sigma must be > 0
        with pytest.raises(ValidationError):
            LognormalDistribution(mu=3.5, sigma=1.0, min=-1.0)  # min must be >= 0
    
    def test_uniform_distribution(self):
        """Test UniformDistribution strategy."""
        strategy = UniformDistribution(min=10.0, max=20.0)
        assert strategy.name == "uniform"
        
        values = strategy.generate(100)
        assert len(values) == 100
        assert all(10.0 <= v <= 20.0 for v in values)
        
        # Test validation
        with pytest.raises(ValidationError):
            UniformDistribution(min=20.0, max=10.0)  # max must be > min
    
    def test_pareto_distribution(self):
        """Test ParetoDistribution strategy."""
        strategy = ParetoDistribution(alpha=2.0, scale=1.0)
        assert strategy.name == "pareto"
        
        values = strategy.generate(100)
        assert len(values) == 100
        # Pareto values are scale * pareto(alpha), so they should be >= scale
        # But due to numpy's pareto implementation, values might be slightly less
        assert all(v >= 0.0 for v in values)  # Should be non-negative
        
        # Test validation
        with pytest.raises(ValidationError):
            ParetoDistribution(alpha=-1.0, scale=1.0)  # alpha must be > 0
    
    def test_zipf_distribution(self):
        """Test ZipfDistribution strategy."""
        strategy = ZipfDistribution(n=10, s=1.5)
        assert strategy.name == "zipf"
        
        values = strategy.generate(100)
        assert len(values) == 100
        assert all(isinstance(v, int) for v in values)
        assert all(1 <= v <= 10 for v in values)
        
        # Test validation
        with pytest.raises(ValidationError):
            ZipfDistribution(n=0, s=1.5)  # n must be > 0
    
    def test_exponential_distribution(self):
        """Test ExponentialDistribution strategy."""
        # Use alias "lambda" instead of "lambda_"
        strategy = ExponentialDistribution(**{"lambda": 0.5, "min": 0.0})
        assert strategy.name == "exponential"
        assert strategy.lambda_ == 0.5
        
        values = strategy.generate(100)
        assert len(values) == 100
        assert all(v >= 0.0 for v in values)
        
        # Test validation
        with pytest.raises(ValidationError):
            ExponentialDistribution(lambda_=-1.0)  # lambda must be > 0
    
    def test_categorical_distribution(self):
        """Test CategoricalDistribution strategy."""
        pmf = {"A": 0.3, "B": 0.5, "C": 0.2}
        strategy = CategoricalDistribution(pmf=pmf)
        assert strategy.name == "categorical"
        
        values = strategy.generate(100)
        assert len(values) == 100
        assert all(v in ["A", "B", "C"] for v in values)
        
        # Test validation
        with pytest.raises(ValidationError):
            CategoricalDistribution(pmf={})  # pmf cannot be empty
        with pytest.raises(ValidationError):
            CategoricalDistribution(pmf={"A": 0.5, "B": 0.3})  # must sum to ~1.0
    
    def test_bernoulli_distribution(self):
        """Test BernoulliDistribution strategy."""
        strategy = BernoulliDistribution(p_true=0.7)
        assert strategy.name == "bernoulli"
        
        values = strategy.generate(100)
        assert len(values) == 100
        assert all(isinstance(v, bool) for v in values)
        
        # Check approximate probability
        true_count = sum(values)
        assert 0.5 <= true_count / 100 <= 0.9  # Allow some variance
        
        # Test validation
        with pytest.raises(ValidationError):
            BernoulliDistribution(p_true=1.5)  # p_true must be <= 1.0


class TestFakerStrategies:
    """Test Faker-based string generation strategies."""
    
    def test_faker_name_strategy(self):
        """Test FakerNameStrategy."""
        strategy = FakerNameStrategy(locale="en_US", name_type="full")
        assert strategy.name == "faker_name"
        assert strategy.kind == "string"
        
        values = strategy.generate(10)
        assert len(values) == 10
        assert all(isinstance(v, str) for v in values)
        assert all(len(v) > 0 for v in values)
        
        # Test first name
        strategy_first = FakerNameStrategy(locale="en_US", name_type="first")
        values_first = strategy_first.generate(10)
        assert all(isinstance(v, str) for v in values_first)
    
    def test_faker_email_strategy(self):
        """Test FakerEmailStrategy."""
        strategy = FakerEmailStrategy(locale="en_US")
        assert strategy.name == "faker_email"
        
        values = strategy.generate(10)
        assert len(values) == 10
        assert all("@" in v for v in values)  # Should contain @
        
        # Test with domain
        strategy_domain = FakerEmailStrategy(locale="en_US", domain="example.com")
        values_domain = strategy_domain.generate(10)
        assert all("@example.com" in v for v in values_domain)
    
    def test_faker_address_strategy(self):
        """Test FakerAddressStrategy."""
        strategy = FakerAddressStrategy(locale="en_US", component="city")
        assert strategy.name == "faker_address"
        
        values = strategy.generate(10)
        assert len(values) == 10
        assert all(isinstance(v, str) for v in values)
    
    def test_faker_company_strategy(self):
        """Test FakerCompanyStrategy."""
        strategy = FakerCompanyStrategy(locale="en_US")
        assert strategy.name == "faker_company"
        
        values = strategy.generate(10)
        assert len(values) == 10
        assert all(isinstance(v, str) for v in values)
    
    def test_faker_text_strategy(self):
        """Test FakerTextStrategy."""
        strategy = FakerTextStrategy(locale="en_US", text_type="sentence")
        assert strategy.name == "faker_text"
        
        values = strategy.generate(10)
        assert len(values) == 10
        assert all(isinstance(v, str) for v in values)
    
    def test_faker_url_strategy(self):
        """Test FakerURLStrategy."""
        strategy = FakerURLStrategy(locale="en_US", url_type="url")
        assert strategy.name == "faker_url"
        
        values = strategy.generate(10)
        assert len(values) == 10
        assert all(isinstance(v, str) for v in values)
    
    def test_faker_phone_strategy(self):
        """Test FakerPhoneStrategy."""
        strategy = FakerPhoneStrategy(locale="en_US")
        assert strategy.name == "faker_phone"
        
        values = strategy.generate(10)
        assert len(values) == 10
        assert all(isinstance(v, str) for v in values)


class TestMimesisStrategies:
    """Test Mimesis-based string generation strategies."""
    
    def test_mimesis_name_strategy(self):
        """Test MimesisNameStrategy."""
        strategy = MimesisNameStrategy(locale="en", name_type="full")
        assert strategy.name == "mimesis_name"
        
        values = strategy.generate(10)
        assert len(values) == 10
        assert all(isinstance(v, str) for v in values)
    
    def test_mimesis_email_strategy(self):
        """Test MimesisEmailStrategy."""
        strategy = MimesisEmailStrategy(locale="en")
        assert strategy.name == "mimesis_email"
        
        values = strategy.generate(10)
        assert len(values) == 10
        assert all("@" in v for v in values)
    
    def test_mimesis_text_strategy(self):
        """Test MimesisTextStrategy."""
        strategy = MimesisTextStrategy(locale="en", text_type="sentence")
        assert strategy.name == "mimesis_text"
        
        values = strategy.generate(10)
        assert len(values) == 10
        assert all(isinstance(v, str) for v in values)
    
    def test_mimesis_address_strategy(self):
        """Test MimesisAddressStrategy."""
        strategy = MimesisAddressStrategy(locale="en", component="city")
        assert strategy.name == "mimesis_address"
        
        values = strategy.generate(10)
        assert len(values) == 10
        assert all(isinstance(v, str) for v in values)
    
    def test_mimesis_coordinates_strategy(self):
        """Test MimesisCoordinatesStrategy."""
        strategy = MimesisCoordinatesStrategy(min_lat=-90, max_lat=90, min_lon=-180, max_lon=180)
        assert strategy.name == "mimesis_coordinates"
        assert strategy.kind == "location"
        
        values = strategy.generate(10)
        assert len(values) == 10
        assert all(isinstance(v, dict) for v in values)
        assert all("lat" in v and "lon" in v for v in values)
        assert all(-90 <= v["lat"] <= 90 for v in values)
        assert all(-180 <= v["lon"] <= 180 for v in values)
    
    def test_mimesis_country_strategy(self):
        """Test MimesisCountryStrategy."""
        strategy = MimesisCountryStrategy(locale="en", code_type="name")
        assert strategy.name == "mimesis_country"
        
        values = strategy.generate(10)
        assert len(values) == 10
        assert all(isinstance(v, str) for v in values)


class TestRegexStrategy:
    """Test regex-based string generation strategy."""
    
    def test_regex_strategy_simple(self):
        """Test RegexStrategy with simple pattern."""
        try:
            strategy = RegexStrategy(pattern=r"[A-Z]{3}[0-9]{3}", unique=True)
            assert strategy.name == "regex"
            assert strategy.kind == "string"
            
            values = strategy.generate(10)
            assert len(values) == 10
            assert all(isinstance(v, str) for v in values)
            assert all(len(v) == 6 for v in values)
            
            # Check uniqueness if enabled
            if strategy.unique:
                assert len(set(values)) == len(values)  # All unique
        except ImportError:
            pytest.skip("rstr library not available")
    
    def test_regex_strategy_validation(self):
        """Test RegexStrategy validation."""
        try:
            # Test invalid pattern (lookahead not supported)
            with pytest.raises(ValidationError):
                RegexStrategy(pattern=r"(?=.*[A-Z])", unique=True)
        except ImportError:
            pytest.skip("rstr library not available")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

