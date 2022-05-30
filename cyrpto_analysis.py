from __future__ import print_function
import requests
from smtplib import SMTP
import time
from builtins import range
from pprint import pprint
from datetime import datetime, date
from airflow.utils.dates import days_ago

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

def get_current_value():
    key = "https://api.binance.com/api/v3/ticker/price?symbol="
    j = 0
    currencies = ["BTCUSDT", "DOGEUSDT", "LTCUSDT"]

    for i in currencies:
        # completing API for request
        url = key + currencies[j]
        data = requests.get(url)
        data = data.json()
        j = j + 1

        with open(f"./dags/{i}.txt", "r") as f:
            prices = f.readlines()

            if len(prices) >= 12:
                prices.pop(0)

            f.close()

        with open(f"./dags/{i}.txt", "w") as f:
            for item in prices:
                f.write(item)

            f.writelines(data['price'] + "\n")

            f.close()

def do_analysis():
    message = ""
    myMailAdress = "*****"
    password = "*****"
    sendTo = "*****"
    subcjet = "Kripto Analiz"

    mail = SMTP("smtp.gmail.com", 587)
    mail.ehlo()
    mail.starttls()
    mail.login(myMailAdress, password)

    currencies = ["BTCUSDT", "DOGEUSDT", "LTCUSDT"]
    for i in currencies:
        with open(f"./dags/{i}.txt") as f:
            a = f.readlines();
        if (100 * float(a[-1]) / float(min(a)) > 100):
            message += f"{i} paritesi son bir saat içerisinde %{round(100 * float(a[-1]) / float(min(a)) - 100, 2)} arttı." + "\n"

        elif (100 * float(a[-1]) / float(max(a)) < 100):
            message += f"{i} paritesi son bir saat içerisinde %{round(100 - 100 * float(a[-1]) / float(max(a)), 2)} azaldı." + "\n"
        else:
            message += f"{i} paritesi son bir saat içerisinde sabit." + "\n"

    content = "Subject: {0}\n\n{1}".format(subcjet,message)
    mail.sendmail(myMailAdress, sendTo, content.encode('utf-8'))

args = {
    'owner': 'Airflow',
}

with DAG(dag_id="crypto_analysis", default_args=args, start_date=datetime(2022, 5, 18), schedule_interval='*/5  * * * *', catchup=False) as dag:
    first_task = PythonOperator(
        task_id='get_current_value',
        python_callable=get_current_value,
    )

    second_task = PythonOperator(
        task_id='do_analysis',
        python_callable=do_analysis,
    )

    [first_task >> second_task]
