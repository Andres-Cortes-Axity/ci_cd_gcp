CREATE TABLE IF NOT EXISTS raw_sales.tb_currency_rejected(
    fecha STRING,
    moneda STRING,
    compra FLOAT64,
    venta FLOAT64,
    origen STRING,
    process_datetime STRING,
    mensaje_rejected STRING
)