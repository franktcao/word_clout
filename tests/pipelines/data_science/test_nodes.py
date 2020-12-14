from src.pipelines.data_science.nodes import parse_location


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
