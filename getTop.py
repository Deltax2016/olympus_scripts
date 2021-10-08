from gql import gql, Client
from gql.transport.aiohttp import AIOHTTPTransport

transport = AIOHTTPTransport(url="https://api.thegraph.com/subgraphs/name/deltax2016/olympus-wallets")
client = Client(transport=transport, fetch_schema_from_transport=True)

def getTopBalances(timestamp_start, timestamp_end, balance_gt):

    queryString = f"""query getTopBalances {{
        dailyBalances(first:1000,orderBy: timestamp, where: {{timestamp_gt: {timestamp_start}, timestamp_lt: {timestamp_end}, ohmBalance_gt: "{balance_gt}"}}) {{
            ohmBalance
            address
            day
            hourBalance(orderBy: timestamp) {{
                ohmBalance
                hour
            }}
        }}
    }}
    """
    # balance before listing
    
    query = gql(queryString)

    result = client.execute(query)
    
    days = {}

    for day in result['dailyBalances']:
        if not (str(day['day']) in days):
            days[str(day['day'])] = []

        tempHours = {}
        for hour in day['hourBalance']:
            tempHours[hour['hour']] = {}
            tempHours[hour['hour']]['ohmBalance'] = hour['ohmBalance']
        days[str(day['day'])].append({
            'wallet': day['address'],
            'ohmBalance': day['ohmBalance'],
            'hourBalances': tempHours
        })


    for day in days:
        for wallet in range(len(days[day])):
            for hour in range(24):
                if not (hour in days[day][wallet]['hourBalances']):
                    if hour !=0:
                        days[day][wallet]['hourBalances'][str(hour)] = {}
                        days[day][wallet]['hourBalances'][str(hour)]['ohmBalance'] = days[day][wallet]['hourBalances'][str(hour-1)]['ohmBalance']
                    else:
                        if str(int(day)-1) in days:
                            days[day][wallet]['hourBalances'][str(hour)] = {}
                            days[day][wallet]['hourBalances'][str(hour)]['ohmBalance'] = days[str(int(day)-1)][wallet]['hourBalances'][str(hour)]['ohmBalance']
                        else:
                            days[day][wallet]['hourBalances'][str(hour)] = {}
                            days[day][wallet]['hourBalances'][str(hour)]['ohmBalance'] = "0"

    return days

timestamp_start = 1617291702
timestamp_end = 1617691702
amount = 100000
res = getTopBalances(timestamp_start, timestamp_end, amount)

print(res['90'][0])





