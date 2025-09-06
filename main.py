#!/usr/bin/env python3
"""
Main entry point for data processing pipeline.

This replaces the old data_processor.py with a clean, modular architecture.
"""

import argparse
import asyncio
import logging
from pathlib import Path
from typing import Dict, Optional
import platform

from src import DataProcessor
from src.diagnostics.metrics import (
    log_settlement_inputs,
    log_level_distribution,
    log_suffix_masking,
    log_account_base_conflicts,
)
from src.config import FIELD_MAPPINGS
from feishu_writer_sync import FeishuWriterV3

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def process_data_files(
    file_paths: Dict[str, str],
    output_path: Optional[str] = None,
    feishu_config: Optional[Dict] = None,
) -> None:
    """
    Process data files through the pipeline.

    Args:
        file_paths: Dictionary mapping source names to file paths
        output_path: Optional output file path
        feishu_config: Optional Feishu configuration for writing results
    """
    logger.info("Starting data processing pipeline")
    
    # 运行时环境诊断
    logger.info("=== 运行时环境诊断 ===")
    logger.info(f"Python版本: {platform.python_version()}")
    logger.info(f"平台: {platform.platform()}")
    logger.info(f"工作目录: {Path.cwd()}")
    
    # 依赖诊断简化：不再检测 python-magic，统一按扩展名解析
    
    logger.info(f"待处理文件数: {len(file_paths)}")
    logger.info("=== 开始处理 ===")

    # Initialize processor with full config (so spending_sheet_names etc. are available)
    processor = DataProcessor(config)

    try:
        # Process all data sources
        result_df = processor.process_pipeline(file_paths)

        # Finalize output names (Chinese standardization)
        try:
            from src.outputs.naming import rename_for_output

            result_df = rename_for_output(result_df)
        except Exception as e:
            logger.warning(f"Output renaming skipped due to error: {e}")

        # Diagnostics (opt-in via env) before settlement
        try:
            log_settlement_inputs(result_df)
            log_level_distribution(result_df)
            log_suffix_masking(result_df, file_paths.keys())
            log_account_base_conflicts(result_df)
        except Exception as e:
            logger.error(f"Diagnostics failed: {e}", exc_info=True)

        # Compute settlement (结算) summary over T/T-1 months
        try:
            from src.analysis.settlement import compute_settlement_cn
            # CLI 没有传入维度参数，默认按经销商ID
            summary_df = compute_settlement_cn(result_df, None)
            result_df = summary_df
        except Exception as e:
            logger.error(f"Settlement computation failed: {e}")

        logger.info(f"Processing complete. Result shape: {result_df.shape}")
        logger.info(f"Result columns: {result_df.columns}")

        # Save to file if specified
        if output_path:
            result_df.write_csv(output_path)
            logger.info(f"Results saved to {output_path}")

        # Write to Feishu if configured
        if feishu_config and feishu_config.get("enabled"):
            writer = FeishuWriterV3(feishu_config)

            # Validate configuration
            validation = await writer.validate_config()
            if validation["valid"]:
                logger.info("Feishu configuration valid, writing results...")

                # Convert DataFrame to records
                records = result_df.to_dicts()
                success = await writer.write_records(records)

                if success:
                    logger.info("Successfully wrote results to Feishu")
                else:
                    logger.error("Failed to write results to Feishu")
            else:
                logger.error(
                    f"Feishu configuration invalid: {validation['errors']}"
                )

        return result_df

    except Exception as e:
        logger.error(f"Data processing failed: {e}")
        raise


def create_sample_config() -> Dict:
    """Create sample configuration for testing."""
    return {
        "video_excel_file": "data/video_data.xlsx",
        "live_bi_file": "data/live_data.xlsx",
        "msg_bi_file": "data/msg_data.xlsx",
        "account_bi_file": "data/account_bi.xlsx",
        "leads_file": "data/leads.xlsx",
        "spending_file": "data/spending.xlsx",
        "account_base_file": "data/account_base.xlsx",
    }


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Data Processing Pipeline")
    parser.add_argument(
        "--config", type=str, help="Configuration file path (JSON format)"
    )
    parser.add_argument(
        "--output", type=str, help="Output file path for results"
    )
    parser.add_argument(
        "--feishu-config",
        type=str,
        help="Feishu configuration file path (JSON format)",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate configuration without processing",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Configure logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    try:
        # Load configuration
        if args.config:
            import json

            with open(args.config, "r", encoding="utf-8") as f:
                config = json.load(f)
        else:
            logger.info("No config file provided, using sample configuration")
            config = create_sample_config()

        # Load Feishu configuration
        feishu_config = None
        if args.feishu_config:
            import json

            with open(args.feishu_config, "r", encoding="utf-8") as f:
                feishu_config = json.load(f)

        # Validate configuration if requested
        if args.validate:
            logger.info("Validating configuration...")

            # Check file paths
            missing_files = []
            for source, path in config.items():
                if not Path(path).exists():
                    missing_files.append(f"{source}: {path}")

            if missing_files:
                logger.error("Missing files:")
                for missing in missing_files:
                    logger.error(f"  {missing}")
                return 1
            else:
                logger.info("All files found, configuration valid")

                # Validate Feishu if configured
                if feishu_config:
                    writer = FeishuWriterV3(feishu_config)
                    validation = asyncio.run(writer.validate_config())
                    if validation["valid"]:
                        logger.info("Feishu configuration valid")
                    else:
                        logger.error(
                            f"Feishu configuration invalid: {validation['errors']}"
                        )
                        return 1

                return 0

        # Run processing
        logger.info("Starting data processing...")
        result_df = asyncio.run(
            process_data_files(config, args.output, feishu_config)
        )

        logger.info("Data processing completed successfully")
        return 0

    except Exception as e:
        logger.error(f"Application failed: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
