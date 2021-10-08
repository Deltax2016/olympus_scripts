from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

transport = AIOHTTPTransport(url="https://api.thegraph.com/subgraphs/name/deltax2016/olympus-wallets")
client = Client(transport=transport, fetch_schema_from_transport=True)

def getFirstWallets(timestamp_start=None, cnt=None):
    days = 365
    hours = 24
    minutes = 60

    queryString = f"""query balancesByWallet {{
        wallets(orderBy: birth, first: {cnt}) {{ 
            id
            dailyBalance(orderBy: timestamp) {{
                ohmBalance
                day
                hourBalance(orderBy: timestamp) {{
                    ohmBalance
                    hour
                    minuteBalance(orderBy: timestamp) {{
                        ohmBalance
                        minute
                    }}
                }}
            }}
        }}
    }}
    """
    # balance before listing
    

    query = gql(queryString)

    result = client.execute(query)

    #print(result['wallets'][0])

    
    wallets = []
    for i in range(cnt):
        tempWallet = {}
        tempWallet['address'] = result['wallets'][i]['id']
        tempWallet['days'] = {}
        for day in result['wallets'][i]['dailyBalance']:
            tempWallet['days'][day['day']] = {}
            tempWallet['days'][day['day']]['ohmBalance'] = day['ohmBalance']
            tempWallet['days'][day['day']]['hours'] = {}
            for hour in day['hourBalance']:
                tempWallet['days'][day['day']]['hours'][hour['hour']] = {}
                tempWallet['days'][day['day']]['hours'][hour['hour']]['ohmBalance'] = hour['ohmBalance']
        wallets.append(tempWallet)

    return wallets

timestamp_start = 1617291702
N = 10
wallet = "0x0822f3c03dcc24d200aff33493dc08d0e1f274a2"
res = getFirstWallets(timestamp_start, N)

print(res[0]['84'])




