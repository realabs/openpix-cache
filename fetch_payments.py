import json, os, time, requests
from datetime import datetime

API_KEY = os.environ.get('OPENPIX_API_KEY', '')

def main():
    payments, skip = [], 0
    while True:
        r = requests.get(f'https://api.openpix.com.br/api/v1/payment?limit=100&skip={skip}',
                        headers={'Authorization': API_KEY})
        if r.status_code != 200: break
        data = r.json().get('payments', [])
        if not data: break
        for p in data:
            pay, tx, dest = p.get('payment',{}), p.get('transaction',{}), p.get('destination',{})
            payments.append({
                'correlationID': pay.get('correlationID',''),
                'value': pay.get('value',0)/100,
                'destinationAlias': pay.get('destinationAlias',''),
                'status': pay.get('status',''),
                'endToEndId': tx.get('endToEndId',''),
                'transactionTime': tx.get('time',''),
                'recipientName': dest.get('name',''),
                'recipientTaxID': dest.get('taxID',''),
                'recipientBank': dest.get('bank',''),
                'recipientBranch': dest.get('branch',''),
                'recipientAccount': dest.get('account',''),
                'recipientPixKey': dest.get('pixKey','') or pay.get('destinationAlias',''),
                'error': p.get('error',{})
            })
        skip += 100
        time.sleep(0.2)
    with open('payments_cache.json','w') as f:
        json.dump({'lastUpdated':datetime.now().isoformat(),'totalPayments':len(payments),'payments':payments},f)
    print(f'Saved {len(payments)} payments')

if __name__=='__main__': main()
