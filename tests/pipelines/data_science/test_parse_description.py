from src.pipelines.data_science.parse_description import convert_description_stats
import pandas as pd


class TestConvertDescriptionStats:
    @staticmethod
    def test_simple_case(mocker, tmp_path):
        # === Arrange
        data_to_test = pd.DataFrame({})
        mocked_called_function = mocker.patch(
            "src.pipelines.data_science.parse_description.extract_description_stats"
        )

        # === Act
        convert_description_stats(data=data_to_test, intermediate_path=tmp_path)

        # === Assert
        mocked_called_function.assert_called_once()
