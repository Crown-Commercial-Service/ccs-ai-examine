import pandas as pd
from flask import Flask, render_template, jsonify
from datetime import datetime

app = Flask(__name__)

def load_suppliers_data():
    df = pd.read_csv('suppliers.csv')
    suppliers_data = {}
    for framework, group in df.groupby('framework'):
        suppliers_list = group.to_dict('records')
        for supplier in suppliers_list:
            contract_start_date = datetime.strptime(supplier['contract_start'], '%Y-%m-%d')
            today = datetime.now()
            months_ran = (today.year - contract_start_date.year) * 12 + (today.month - contract_start_date.month)
            supplier['details'] = {
                'Buyer name': supplier.pop('buyer_name'),
                'Contract value': supplier.pop('contract_value'),
                'Contract start': supplier.pop('contract_start'),
                'Contract end': supplier.pop('contract_end'),
                'Reported spend': supplier.pop('reported_spend'),
                'Months Run So Far': months_ran,
            }
        suppliers_data[framework] = suppliers_list
    return suppliers_data

suppliers_data = load_suppliers_data()

@app.route('/')
def index():
    frameworks = list(suppliers_data.keys())
    initial_suppliers = suppliers_data[frameworks[0]]
    return render_template('index.html', frameworks=frameworks, suppliers=initial_suppliers)

@app.route('/suppliers/<framework>')
def get_suppliers(framework):
    suppliers = suppliers_data.get(framework, [])
    return jsonify(suppliers)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8080)
