if [ x"${PORTFOLIO_DATA}" == "x" ]; then
     gunicorn --bind 0.0.0.0:5000 app:app
  else
     python app.py >/airflow/xcom/return.json
fi