from steps.settings import GlobalConfig

def generate_inference_dataset_etl_query(cfg: GlobalConfig, target_date: str) -> str:
    return f"""
    SELECT
        cust_id,
        age,
        gender,
        purchase_date
    FROM {cfg.athena.customer}
    WHERE purchase_date = DATE('{target_date}')
    """
