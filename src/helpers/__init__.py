"""Helper modules for LUMINO MCP Server."""

from .utils import (
    calculate_duration,
    parse_time_period,
    parse_time_parameters,
    format_yaml_output,
    format_detailed_output,
    format_summary_output,
    calculate_context_tokens,
    get_all_pod_logs,
    clean_pipeline_logs,
    calculate_utilization,
    list_pods,
    detect_anomalies_in_data,
    # Log analysis helpers
    extract_error_patterns,
    categorize_errors,
    generate_log_summary,
    # Pipeline analysis helpers
    determine_root_cause,
    recommend_actions,
    get_pipeline_details,
    get_task_details,
    # Resource search helpers
    build_advanced_label_selector,
    get_resource_api_info,
    extract_resource_info,
    analyze_labels,
    calculate_namespace_distribution,
    sort_resources,
    # Certificate parsing helpers
    parse_certificate,
    categorize_certificate_status,
    # Performance analysis helpers
    detect_performance_trend,
    # Topology graph conversion helpers
    convert_to_graphviz,
    convert_to_mermaid,
    # Resource forecasting helpers
    calculate_forecast_intervals,
    simple_linear_forecast,
    # Simulation helpers
    convert_duration_to_seconds,
    convert_duration_to_hours,
    calculate_std_dev,
    calibrate_simulation_models,
    run_monte_carlo_simulation,
    collect_baseline_system_data,
    build_system_behavior_models,
    load_historical_performance_data,
)

from .constants import (
    SMART_EVENTS_CONFIG,
    LOG_ANALYSIS_CONFIG,
    PIPELINE_ANALYSIS_CONFIG,
    SEMANTIC_SEARCH_CONFIG,
)

from .semantic_search import (
    # Query interpretation
    interpret_semantic_query,
    # Search strategy
    determine_search_strategy,
    # Entity extraction
    extract_k8s_entities,
    # Semantic matching
    find_semantic_matches,
    calculate_semantic_relevance,
    identify_match_reasons,
    extract_log_metadata,
    # Result ranking
    rank_results_by_semantic_relevance,
    # Pattern identification
    identify_common_patterns,
    analyze_severity_distribution,
    # Suggestions
    generate_semantic_suggestions,
    # Async search helpers
    _build_log_params,
    _get_target_namespaces,
    _search_pod_logs_semantically,
    _search_events_semantically,
    _search_tekton_resources_semantically,
)

from .log_analysis import (
    # Strategy Classes
    LogAnalysisStrategy,
    LogAnalysisContext,
    AnalysisCache,
    StrategySelector,
    LogStreamProcessor,
    # Pattern extraction functions
    extract_log_patterns,
    extract_timestamp,
    assess_log_severity,
    sample_logs_by_time,
    # Streaming analysis functions
    generate_streaming_summary,
    analyze_trending_patterns,
    generate_streaming_recommendations,
    # Combination functions
    combine_analysis_results,
    generate_supplementary_insights,
    generate_hybrid_recommendations,
    # Summary functions
    generate_focused_summary,
    get_strategy_selection_reason,
    # ML/Data processing functions
    preprocess_log_data,
    calculate_entropy,
    extract_log_features,
    train_anomaly_model,
    analyze_log_patterns_for_failure_prediction,
    generate_failure_predictions,
    # Token limit truncation functions
    truncate_to_token_limit,
    truncate_streaming_results,
    # Utility functions
    analysis_cache,
)

from .event_analysis import (
    EventSeverity,
    EventCategory,
    ProgressiveEventAnalyzer,
    MLPatternDetector,
    LogMetricsIntegrator,
    RunbookSuggestionEngine,
    classify_event_severity_from_string,
    classify_event_category_from_string,
    calculate_relevance_score_from_string,
    extract_timestamp_from_string,
    estimate_string_event_tokens,
    smart_sample_string_events,
    generate_string_events_summary,
    generate_string_events_insights,
    generate_string_events_recommendations,
    assess_overall_risk,
    generate_strategic_recommendations,
    generate_comprehensive_insights,
)

from .failure_analysis import (
    # Failure context identification
    identify_failure_context,
    # Specific failure analysis
    analyze_pipeline_failure,
    analyze_pod_failure,
    analyze_generic_failure,
    # Timeline and related failures
    build_failure_timeline,
    find_related_failures,
    # Advanced RCA
    perform_advanced_rca,
    # Resource and configuration analysis
    analyze_resource_constraints,
    analyze_configuration_issues,
    analyze_pipeline_dependencies,
    # Remediation planning
    generate_remediation_plan,
    # Confidence and scoring
    calculate_confidence_score,
    calculate_failure_impact_score,
    # Utility functions
    extract_log_content_string,
    get_category_description,
    analyze_error_patterns,
    # Trend analysis
    analyze_failure_trends,
    calculate_trend_confidence,
    # Severity assessment
    assess_failure_severity,
    get_response_time_recommendation,
    # Simulation impact analysis
    categorize_impact_severity,
    generate_performance_impact_description,
    generate_reliability_impact_description,
    generate_cost_impact_description,
    analyze_system_impact,
    perform_risk_assessment,
    calculate_simulation_quality,
    generate_simulation_recommendations,
)

from .resource_topology import (
    # Multi-cluster client management
    get_multi_cluster_clients,
    # Pipeline correlation and tracking
    correlate_pipeline_events,
    matches_trace_identifier,
    get_pipeline_status,
    extract_task_info,
    in_time_range,
    # Artifact tracking
    track_artifacts,
    extract_pipeline_artifacts,
    # Bottleneck detection
    analyze_bottlenecks,
    # Machine config pool analysis
    analyze_machine_config_pool_status,
    detect_pool_issues,
    generate_update_recommendations,
    # Operator analysis
    analyze_operator_dependencies,
    identify_critical_issues,
    analyze_operator_conditions,
    # Topology mapping utilities
    get_multi_cluster_topology_clients,
    generate_node_id,
    calculate_dependency_weight,
    get_resource_metrics,
    analyze_owner_references,
    analyze_service_dependencies,
    analyze_volume_dependencies,
    # Simulation affected components
    identify_affected_components,
)

__all__ = [
    # Utils
    "calculate_duration",
    "parse_time_period",
    "parse_time_parameters",
    "format_yaml_output",
    "format_detailed_output",
    "format_summary_output",
    "calculate_context_tokens",
    "get_all_pod_logs",
    "clean_pipeline_logs",
    "calculate_utilization",
    "list_pods",
    "detect_anomalies_in_data",
    # Log analysis helpers
    "extract_error_patterns",
    "categorize_errors",
    "generate_log_summary",
    # Pipeline analysis helpers
    "determine_root_cause",
    "recommend_actions",
    "get_pipeline_details",
    "get_task_details",
    # Resource search helpers
    "build_advanced_label_selector",
    "get_resource_api_info",
    "extract_resource_info",
    "analyze_labels",
    "calculate_namespace_distribution",
    "sort_resources",
    # Certificate parsing helpers
    "parse_certificate",
    "categorize_certificate_status",
    # Performance analysis helpers
    "detect_performance_trend",
    # Constants
    "SMART_EVENTS_CONFIG",
    "LOG_ANALYSIS_CONFIG",
    "PIPELINE_ANALYSIS_CONFIG",
    "SEMANTIC_SEARCH_CONFIG",
    # Semantic Search
    "interpret_semantic_query",
    "determine_search_strategy",
    "extract_k8s_entities",
    "find_semantic_matches",
    "calculate_semantic_relevance",
    "identify_match_reasons",
    "extract_log_metadata",
    "rank_results_by_semantic_relevance",
    "identify_common_patterns",
    "analyze_severity_distribution",
    "generate_semantic_suggestions",
    "_build_log_params",
    "_get_target_namespaces",
    "_search_pod_logs_semantically",
    "_search_events_semantically",
    "_search_tekton_resources_semantically",
    # Log Analysis
    "LogAnalysisStrategy",
    "LogAnalysisContext",
    "AnalysisCache",
    "StrategySelector",
    "LogStreamProcessor",
    "extract_log_patterns",
    "extract_timestamp",
    "assess_log_severity",
    "sample_logs_by_time",
    "generate_streaming_summary",
    "analyze_trending_patterns",
    "generate_streaming_recommendations",
    "combine_analysis_results",
    "generate_supplementary_insights",
    "generate_hybrid_recommendations",
    "generate_focused_summary",
    "get_strategy_selection_reason",
    "preprocess_log_data",
    "calculate_entropy",
    "extract_log_features",
    "train_anomaly_model",
    "analyze_log_patterns_for_failure_prediction",
    "generate_failure_predictions",
    "truncate_to_token_limit",
    "truncate_streaming_results",
    "analysis_cache",
    # Event Analysis
    "EventSeverity",
    "EventCategory",
    "ProgressiveEventAnalyzer",
    "MLPatternDetector",
    "LogMetricsIntegrator",
    "RunbookSuggestionEngine",
    "classify_event_severity_from_string",
    "classify_event_category_from_string",
    "calculate_relevance_score_from_string",
    "extract_timestamp_from_string",
    "estimate_string_event_tokens",
    "smart_sample_string_events",
    "generate_string_events_summary",
    "generate_string_events_insights",
    "generate_string_events_recommendations",
    "assess_overall_risk",
    "generate_strategic_recommendations",
    "generate_comprehensive_insights",
    # Failure Analysis
    "identify_failure_context",
    "analyze_pipeline_failure",
    "analyze_pod_failure",
    "analyze_generic_failure",
    "build_failure_timeline",
    "find_related_failures",
    "perform_advanced_rca",
    "analyze_resource_constraints",
    "analyze_configuration_issues",
    "analyze_pipeline_dependencies",
    "generate_remediation_plan",
    "calculate_confidence_score",
    "calculate_failure_impact_score",
    "extract_log_content_string",
    "get_category_description",
    "analyze_error_patterns",
    "analyze_failure_trends",
    "calculate_trend_confidence",
    "assess_failure_severity",
    "get_response_time_recommendation",
    # Resource Topology
    "get_multi_cluster_clients",
    "correlate_pipeline_events",
    "matches_trace_identifier",
    "get_pipeline_status",
    "extract_task_info",
    "in_time_range",
    "track_artifacts",
    "extract_pipeline_artifacts",
    "analyze_bottlenecks",
    # Machine Config Pool Analysis
    "analyze_machine_config_pool_status",
    "detect_pool_issues",
    "generate_update_recommendations",
    # Operator Analysis
    "analyze_operator_dependencies",
    "identify_critical_issues",
    "analyze_operator_conditions",
    # Topology Mapping Utilities
    "get_multi_cluster_topology_clients",
    "generate_node_id",
    "calculate_dependency_weight",
    "get_resource_metrics",
    "analyze_owner_references",
    "analyze_service_dependencies",
    "analyze_volume_dependencies",
    # Topology Graph Conversion
    "convert_to_graphviz",
    "convert_to_mermaid",
    # Resource Forecasting
    "calculate_forecast_intervals",
    "simple_linear_forecast",
    # Simulation Helpers
    "convert_duration_to_seconds",
    "convert_duration_to_hours",
    "calculate_std_dev",
    "calibrate_simulation_models",
    "run_monte_carlo_simulation",
    "collect_baseline_system_data",
    "build_system_behavior_models",
    "load_historical_performance_data",
    # Simulation Impact Analysis
    "categorize_impact_severity",
    "generate_performance_impact_description",
    "generate_reliability_impact_description",
    "generate_cost_impact_description",
    "analyze_system_impact",
    "perform_risk_assessment",
    "calculate_simulation_quality",
    "generate_simulation_recommendations",
    # Simulation Affected Components
    "identify_affected_components",
]
