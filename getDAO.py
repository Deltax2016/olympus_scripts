from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

transport = AIOHTTPTransport(url="https://api.thegraph.com/subgraphs/name/deltax2016/olympus-wallets")
client = Client(transport=transport, fetch_schema_from_transport=True)

def getBalance(address=None, timestamp_start=None, timestamp_end=None):

    queryString = f"""query getDAO {{
        dailyBalances(orderBy: timestamp, where: {{address: "{address}",timestamp_gt: {timestamp_start}, timestamp_lt: {timestamp_end}}}) {{
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
    """
    # balance before listing
    

    query = gql(queryString)

    result = client.execute(query)
    

    tempWallet = {}
    tempWallet['address'] = address
    tempWallet['days'] = {}
    for day in result['dailyBalances']:
        tempWallet['days'][day['day']] = {}
        tempWallet['days'][day['day']]['ohmBalance'] = day['ohmBalance']
        tempWallet['days'][day['day']]['hours'] = {}
        for hour in day['hourBalance']:
            tempWallet['days'][day['day']]['hours'][hour['hour']] = {}
            tempWallet['days'][day['day']]['hours'][hour['hour']]['ohmBalance'] = hour['ohmBalance']

    for day in range(365):
        if str(day) in tempWallet['days']:
            for hour in range(24):
                if str(hour) in tempWallet['days'][str(day)]['hours']:
                    pass
                else:
                    tempWallet['days'][str(day)]['hours'][str(hour)] = {}
                    if hour != 0:
                        if day!= 0:
                            tempWallet['days'][str(day)]['hours'][str(hour)]['ohmBalance'] = tempWallet['days'][str(day)]['hours'][str(hour-1)]['ohmBalance']
                    else:
                        tempWallet['days'][str(day)]['hours'][str(hour)]['ohmBalance'] = tempWallet['days'][str(day-1)]['ohmBalance']
        else:
            if day != 0:
                tempWallet['days'][str(day)] = {}
                tempWallet['days'][str(day)]['ohmBalance'] = tempWallet['days'][str(day-1)]['ohmBalance']
                tempWallet['days'][str(day)]['hours'] = {}
            else:
                tempWallet['days'][str(day)] = {}
                tempWallet['days'][str(day)]['ohmBalance'] = "0"
                tempWallet['days'][str(day)]['hours'] = {}
            for hour in range(24):
                tempWallet['days'][str(day)]['hours'][str(hour)] = {}
                tempWallet['days'][str(day)]['hours'][str(hour)]['ohmBalance'] = tempWallet['days'][str(day)]['ohmBalance']

    return tempWallet

N = 10
timestamp_start = 1617291702
timestamp_end = 1617691702
wallet = "0x245cc372C84B3645Bf0Ffe6538620B04a217988B"
res = getBalance(wallet, timestamp_start, timestamp_end)

print(res['days']['282'])






