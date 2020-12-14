import pandas as pd
from src.pipelines.data_science.nodes import (
    expand_location,
    parse_location,
    expand_salary,
    parse_salary
)


class TestExpandSalary:
    @staticmethod
    def test_typical():
        pass


class TestParseSalary:
    @staticmethod
    def test_typical_yearly():
        """Assert expected return for typical yearly, ranged input."""
        # === Arrange
        salary_to_test = "$90,000 - $130,000 a year"
        expected = [90_000, 130_000]

        # === Act
        actual = parse_salary(salary_to_test)

        # === Assert
        assert actual == expected

    @staticmethod
    def test_typical_hourly():
        """Assert expected return for typical hourly, ranged input."""
        # === Arrange
        salary_to_test = "$30 - $70 an hour"
        expected = [30 * 2_080, 70 * 2_080]

        # === Act
        actual = parse_salary(salary_to_test)

        # === Assert
        assert actual == expected

    @staticmethod
    def test_single_value_yearly():
        """Assert expected return for typical yearly, single input."""
        # === Arrange
        salary_to_test = "$106,700 a year"
        expected = [106_700, 106_700]

        # === Act
        actual = parse_salary(salary_to_test)

        # === Assert
        assert actual == expected

    @staticmethod
    def test_single_value_hourly():
        """Assert expected return for typical hourly, single input."""
        # === Arrange
        salary_to_test = "$45 an hour"
        expected_value = 45 * 2_080
        expected = 2 * [expected_value]

        # === Act
        actual = parse_salary(salary_to_test)

        # === Assert
        assert actual == expected


class TestExpandLocation:
    @staticmethod
    def test_typical():
        # === Arrange
        df_under_test = pd.DataFrame(
            {
                "comapny_name": 10 * ["Aperture Laboratories"],
                "location": 10 * ["Boston, Ma 02118 (South End area)"],
            }
        )
        expected = pd.DataFrame(
            {
                "comapny_name": 10 * ["Aperture Laboratories"],
                "city": 10 * ["Boston"],
                "state": 10 * ["Ma"],
                "zip_code": 10 * ["02118"],
                "neighborhood": 10 * ["South End"],
            }
        )
        # === Act
        actual = expand_location(df_under_test)

        # === Assert
        pd.testing.assert_frame_equal(actual, expected)


class TestParseLocation:
    @staticmethod
    def test_typical():
        """Assert expected return for typical input."""
        # === Arrange
        location_to_test = "Boston, MA 02118 (South End area)"
        expected = ["Boston", "MA", "02118", "South End"]

        # === Act
        actual = parse_location(location_to_test)

        # === Assert
        assert actual == expected

    @staticmethod
    def test_no_neighborhood():
        """Assert expected is returned when there's no neighborhood."""
        # === Arrange
        location_to_test = "Boston, MA 02118"
        expected = ["Boston", "MA", "02118", None]

        # === Act
        actual = parse_location(location_to_test)

        # === Assert
        assert actual == expected

    @staticmethod
    def test_no_zip_code():
        """Assert expected is returned when there's no zip code."""
        # === Arrange
        location_to_test = "Boston, MA (South End area)"
        expected = ["Boston", "MA", None, "South End"]

        # === Act
        actual = parse_location(location_to_test)

        # === Assert
        assert actual == expected

    @staticmethod
    def test_only_city_state():
        """Assert expected is returned when there's only city and state."""
        # === Arrange
        location_to_test = "Boston, MA"
        expected = ["Boston", "MA", None, None]

        # === Act
        actual = parse_location(location_to_test)

        # === Assert
        assert actual == expected

    @staticmethod
    def test_neighborhood_first():
        """Assert expected is returned when neighborhood is first."""
        # === Arrange
        location_to_test = "Boston, MA (South End area) 02118"
        expected = ["Boston", "MA", "02118", "South End"]

        # === Act
        actual = parse_location(location_to_test)

        # === Assert
        assert actual == expected
