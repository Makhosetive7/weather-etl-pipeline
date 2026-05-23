"""Unit tests for ETL pipeline orchestration in main.py."""

from unittest.mock import MagicMock, patch

from main import EXIT_FAILURE, EXIT_PARTIAL, EXIT_SUCCESS, run_etl_pipeline


def _mock_pipeline_components(
    *,
    db_ok=True,
    raw_data=None,
    transformed_data=None,
    load_result=None,
):
    raw_data = raw_data if raw_data is not None else [{"name": "London"}]
    transformed_data = (
        transformed_data if transformed_data is not None else [{"city": {"city_name": "London"}}]
    )
    load_result = (
        load_result
        if load_result is not None
        else {
            "success": 1,
            "failure": 0,
            "total": 1,
        }
    )

    mock_extractor = MagicMock()
    mock_extractor.fetch_weather_for_cities.return_value = raw_data

    mock_transformer = MagicMock()
    mock_transformer.transform_multiple_records.return_value = transformed_data

    mock_loader = MagicMock()
    mock_loader.test_connection.return_value = db_ok
    mock_loader.load_multiple_records.return_value = load_result

    mock_tracker = MagicMock()

    return mock_extractor, mock_transformer, mock_loader, mock_tracker


@patch("main.EtlRunTracker")
@patch("main.WeatherLoader")
@patch("main.WeatherTransformer")
@patch("main.WeatherExtractor")
@patch("main.Config")
class TestRunEtlPipeline:

    def test_happy_path(
        self,
        mock_config,
        mock_extractor_cls,
        mock_transformer_cls,
        mock_loader_cls,
        mock_tracker_cls,
    ):
        mock_config.validate.return_value = True
        mock_extractor, mock_transformer, mock_loader, mock_tracker = _mock_pipeline_components()
        mock_extractor_cls.return_value = mock_extractor
        mock_transformer_cls.return_value = mock_transformer
        mock_loader_cls.return_value = mock_loader
        mock_tracker_cls.return_value = mock_tracker

        assert run_etl_pipeline() == EXIT_SUCCESS
        mock_tracker.start_run.assert_called_once()
        mock_tracker.finish_run.assert_called_once()
        mock_loader.close.assert_called_once()

    def test_partial_load_failure(
        self,
        mock_config,
        mock_extractor_cls,
        mock_transformer_cls,
        mock_loader_cls,
        mock_tracker_cls,
    ):
        mock_config.validate.return_value = True
        _, _, mock_loader, mock_tracker = _mock_pipeline_components(
            load_result={"success": 1, "failure": 1, "total": 2}
        )
        mock_extractor_cls.return_value = MagicMock()
        mock_extractor_cls.return_value.fetch_weather_for_cities.return_value = [{"a": 1}]
        mock_transformer_cls.return_value = MagicMock()
        mock_transformer_cls.return_value.transform_multiple_records.return_value = [{"b": 1}]
        mock_loader_cls.return_value = mock_loader
        mock_tracker_cls.return_value = mock_tracker

        assert run_etl_pipeline() == EXIT_PARTIAL

    def test_database_connection_fails(
        self,
        mock_config,
        mock_extractor_cls,
        mock_transformer_cls,
        mock_loader_cls,
        mock_tracker_cls,
    ):
        mock_config.validate.return_value = True
        mock_extractor, _, mock_loader, mock_tracker = _mock_pipeline_components(db_ok=False)
        mock_extractor_cls.return_value = mock_extractor
        mock_transformer_cls.return_value = MagicMock()
        mock_loader_cls.return_value = mock_loader
        mock_tracker_cls.return_value = mock_tracker

        assert run_etl_pipeline() == EXIT_FAILURE
        mock_extractor.fetch_weather_for_cities.assert_not_called()

    def test_no_raw_data(
        self,
        mock_config,
        mock_extractor_cls,
        mock_transformer_cls,
        mock_loader_cls,
        mock_tracker_cls,
    ):
        mock_config.validate.return_value = True
        mock_extractor, mock_transformer, mock_loader, mock_tracker = _mock_pipeline_components(
            raw_data=[]
        )
        mock_extractor_cls.return_value = mock_extractor
        mock_transformer_cls.return_value = mock_transformer
        mock_loader_cls.return_value = mock_loader
        mock_tracker_cls.return_value = mock_tracker

        assert run_etl_pipeline() == EXIT_FAILURE

    def test_config_validation_failure(
        self,
        mock_config,
        mock_extractor_cls,
        mock_transformer_cls,
        mock_loader_cls,
        mock_tracker_cls,
    ):
        mock_config.validate.side_effect = ValueError("OPENWEATHER_API_KEY not set")
        mock_tracker_cls.return_value = MagicMock()

        assert run_etl_pipeline() == EXIT_FAILURE
        mock_extractor_cls.assert_not_called()
