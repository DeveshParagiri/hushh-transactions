import os
from fastapi import FastAPI , Request ,  Header
from google.cloud import bigquery
import requests

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'config.json'


app= FastAPI()
client= bigquery.Client()

@app.get("/")
async def read_root():
    return {"message": "Welcome to our extensive network of Financial APIs"}

# @app.get("/get_details/{card_holder}")
# async def get_transactions_by_card_holder(card_holder: str):

#     sql_query = f"""
#     SELECT *
#     FROM hushone-app.lvmh_demo.transactions
#     WHERE card_holder = "{card_holder}"
#     """
#     query_job=client.query(sql_query)
#     results = [dict(row) for row in query_job]

#     if not results:
#         return {"message": "No transactions found for the specified card holder."}

#     return results

@app.get("/basic_info/{card_holder}")
async def get_basic_info(card_holder: str , request: Request , authentication : str = Header(None)):
    """ Test Value for card holder : William Harris \n
    Test Vlaue for authoriztation token : Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"""
    if not authentication:
            return {"status" : 0, "message": f"Authoriztion Token Missing"}
    valid_auth = auth_check(authentication)
    if valid_auth.get('status') != 1:
        return valid_auth
    

    sql_query = f"""
    SELECT
        account_id,
        card_type,
        card_last_4_digits,
    FROM hushone-app.lvmh_demo.transactions
    WHERE card_holder = "{card_holder}"
    """
    query_job = client.query(sql_query)
    results = [dict(row) for row in query_job]
    
    if not results:
            return {"message": f"No account information found for the specified card holder '{card_holder}'."}
    return results

@app.get("/average_transaction_amount/{card_holder}")
async def get_avg_txn_amt(card_holder: str , request: Request, authentication : str = Header(None)):
    """ Test Value for card holder : William Harris \n
    Test Vlaue for authoriztation token : Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"""
    if not authentication:
            return {"status" : 0, "message": f"Authoriztion Token Missing"}
    valid_auth = auth_check(authentication)
    if valid_auth.get('status') != 1:
        return valid_auth
    sql_query = f"""
    SELECT amount
    FROM hushone-app.lvmh_demo.transactions
    WHERE card_holder = "{card_holder}"
    """
    query_job=client.query(sql_query)

    transaction_amounts = [row.amount for row in query_job]
    total_amount= sum(transaction_amounts)
    average= total_amount/len(transaction_amounts) if len(transaction_amounts) > 0 else 0

    return {"average_transaction_amount" : average}

@app.get("/card_types/{card_holder}")
async def get_card_types_by_card_holder(card_holder: str, request: Request , authentication : str = Header(None)):
    """ Test Value for card holder : William Harris \n
    Test Vlaue for authoriztation token : Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"""
    if not authentication:
            return {"status" : 0, "message": f"Authoriztion Token Missing"}
    valid_auth = auth_check(authentication)
    if valid_auth.get('status') != 1:
        return valid_auth
    sql_query= f"""
    SELECT DISTINCT card_type
    FROM hushone-app.lvmh_demo.transactions
    WHERE card_holder = "{card_holder}"
    """

    query_job =  client.query(sql_query)

    card_types = [row.card_type for row in query_job]
    if not card_types:
        return{"message": f"No card types found for the specified card holder '{card_holder}'."}
    return {"card_types": card_types}

        
@app.get("/transaction_cities/{card_holder}")
async def get_transaction_cities(card_holder: str, request: Request , authentication : str = Header(None)):
    """ Test Value for card holder : William Harris \n
    Test Vlaue for authoriztation token : Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"""
    if not authentication:
            return {"status" : 0, "message": f"Authoriztion Token Missing"}
    valid_auth = auth_check(authentication)
    if valid_auth.get('status') != 1:
        return valid_auth
    sql_query = f"""
    SELECT DISTINCT transaction_city
    FROM hushone-app.lvmh_demo.transactions
    WHERE card_holder = "{card_holder}"
    """

    query_job = client.query(sql_query)
    transaction_cities = [row.transaction_city for row in query_job]

    if not transaction_cities:
            return {"message": f"No shipping cities found for the specified card holder '{card_holder}'."}
    return {"shipping_cities": transaction_cities}


def auth_check(token):
    if token:
        parts = token.split()
        if len(parts) == 2 and parts[0].lower() == 'bearer':
            token = parts[1]
            # You now have the Bearer token, and you can perform any necessary authentication or authorization checks.
            if token == "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9":
                return {'status' : 1 , 'response':f"Bearer token: {token}"}
            else:
                return {'status' : 0 , 'response':f"Invalid Token"}
        else:
            return {'status' : 2 , 'response':f"Invalid Authorization header format"}
    else:
        return  {'status' : 2 , 'response':f"Token Missing"}


def month_number_to_word(month_number):
    month_names = [
        "January", "February", "March", "April",
        "May", "June", "July", "August",
        "September", "October", "November", "December"
    ]
    return month_names[month_number - 1] if 1 <= month_number <= 12 else "Invalid Month"

@app.get("/purchase_months/{card_holder}")
async def get_purchase_months(card_holder: str, request: Request , authentication : str = Header(None)):
    """ Test Value for card holder : William Harris \n
    Test Vlaue for authoriztation token : Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"""
    if not authentication:
            return {"status" : 0, "message": f"Authoriztion Token Missing"}
    valid_auth = auth_check(authentication)
    if valid_auth.get('status') != 1:
        return valid_auth
    sql_query = f"""
    SELECT DISTINCT EXTRACT(MONTH FROM date) AS purchase_month
    FROM hushone-app.lvmh_demo.transactions
    WHERE card_holder = "{card_holder}"
    """
    query_job = client.query(sql_query)

    rows = list(query_job)
    purchase_months = [month_number_to_word(row.purchase_month) for row in rows]
    
    if not purchase_months:
        return {"message": f"No purchase months found for the specified card holder '{card_holder}'."}

    return {"purchase_months": purchase_months}




@app.get("/holistic_spend_analysis/{card_holder}")
async def get_holistic_spend_analysis(card_holder: str, request: Request , authentication : str = Header(None)):
    """ Test Value for card holder : William Harris \n
    Test Vlaue for authoriztation token : Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"""
    if not authentication:
            return {"status" : 0, "message": f"Authoriztion Token Missing"}
    valid_auth = auth_check(authentication)
    if valid_auth.get('status') != 1:
        return valid_auth
    mcc_descriptions = {
    "5541": "Brand Websites",
    "5812": "Restaurants",
    "5411": "Supermarkets",
    "5699": "Multi-Brand Websites",
    "4722": "Travel Agencies"
}
    mccs = ["5541", "5812", "5411", "5699", "4722"]
    
    
    sql_query = f"""
    SELECT merchant_category_code AS mcc,
    COUNT(*) AS count
    FROM hushone-app.lvmh_demo.transactions
    WHERE card_holder = "{card_holder}" AND merchant_category_code IN ({",".join(mccs)})
    GROUP BY merchant_category_code
    """
     
    query_job = client.query(sql_query)
    results = [dict(row) for row in query_job]

    if not results:
        return {"message": f"No MCC information found for the specified card holder '{card_holder}'."}
    
    total_count = sum([result["count"] for result in results])

    percentages = [
            {"description": mcc_descriptions.get(str(result["mcc"])), "percentage": (result["count"] / total_count) * 100}
            for result in results
        ]
    return percentages

# @app.get("/payment_channel/{card_holder}")
# async def get_payment_channel_percentage(card_holder: str):
#     sql_query = f"""
#     WITH channel_counts AS (
#         SELECT
#             merchant_category_code AS mcc,
#             payment_channel,
#             COUNT(*) AS channel_count
#         FROM hushone-app.lvmh_demo.transactions
#         WHERE card_holder = "{card_holder}"
#         GROUP BY merchant_category_code, payment_channel
#     ),
#     mcc_totals AS (
#         SELECT
#             merchant_category_code AS mcc,
#             SUM(channel_count) AS total_count
#         FROM channel_counts
#         GROUP BY merchant_category_code
#     )
#     SELECT
#         c.mcc,
#         c.payment_channel,
#         (c.channel_count / t.total_count) * 100 AS percentage
#     FROM channel_counts c
#     JOIN mcc_totals t ON c.mcc = t.mcc
#     ORDER BY c.mcc, c.payment_channel
#     """


     


@app.get("/brand_affiliations/{card_holder}")
async def get_brand_affiliations(card_holder: str, request: Request , authentication : str = Header(None)):
    """ Test Value for card holder : William Harris \n
    Test Vlaue for authoriztation token : Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"""
    if not authentication:
            return {"status" : 0, "message": f"Authoriztion Token Missing"}
    valid_auth = auth_check(authentication)
    if valid_auth.get('status') != 1:
        return valid_auth
    sql_query = f"""
    SELECT DISTINCT merchant_name
    FROM hushone-app.lvmh_demo.transactions
    WHERE card_holder = "{card_holder}"
    """
    query_job = client.query(sql_query)
    shopped_brands = [row.merchant_name for row in query_job]

    if not shopped_brands:
        return {"message": f"No brands found for the specified card holder '{card_holder}'."}

    return {"The customer has recently bought from": shopped_brands}
        
@app.get("/budget_information/{card_holder}")
async def get_budget_information(card_holder: str, request: Request , authentication : str = Header(None)):
    sql_query = f"""
    SELECT
        MAX(amount) AS max_spend,
        MIN(amount) AS min_spend
    FROM hushone-app.lvmh_demo.transactions
    WHERE card_holder = "{card_holder}"
    """
    query_job = client.query(sql_query)
    results = [dict(row) for row in query_job]

    if not results:
            return {"message": f"No spending information found for the specified card holder '{card_holder}'."}
    return results

           
# @app.get("/spend_statistics/{card_holder}")
# async def get_spend_statistics(card_holder: str):
#     return{ Access detailed statistics on spending patterns, including trends and outliers.}

@app.get("/travel_spends/{card_holder}")
async def get_travel_spends(card_holder: str, request: Request , authentication : str = Header(None)):
    """ Test Value for card holder : William Harris \n
    Test Vlaue for authoriztation token : Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"""
    if not authentication:
            return {"status" : 0, "message": f"Authoriztion Token Missing"}
    valid_auth = auth_check(authentication)
    if valid_auth.get('status') != 1:
        return valid_auth
    sql_query = f"""
    SELECT SUM(amount) AS total_travel_spend
    FROM hushone-app.lvmh_demo.transactions
    WHERE card_holder = "{card_holder}" AND merchant_category_code = "4722"
    """
    query_job = client.query(sql_query)
    result = query_job.result()
    total_travel_spend = 0.0

    for row in result:
        total_travel_spend = row.total_travel_spend

    return {"total_travel_spend": total_travel_spend}

@app.get("/travel_destinations/{card_holder}")
async def get_travel_destinations(card_holder: str, request: Request , authentication : str = Header(None)):
    """ Test Value for card holder : William Harris \n
    Test Vlaue for authoriztation token : Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"""
    if not authentication:
            return {"status" : 0, "message": f"Authoriztion Token Missing"}
    valid_auth = auth_check(authentication)
    if valid_auth.get('status') != 1:
        return valid_auth
    sql_query = f"""
    SELECT currency AS currency_code, SUM(amount) AS total_amount
    FROM hushone-app.lvmh_demo.transactions
    WHERE card_holder = "{card_holder}"
    GROUP BY currency
    """
    query_job = client.query(sql_query)
    results = [dict(row) for row in query_job]

    if not results:
        return {"message": f"No currency spending information found for the specified card holder '{card_holder}'."}

    return results



# @app.get("/user_consent/{card_holder}")
# async def get_user_consent(card_holder: str):
#     return{" Confirm the user's consent status for sharing specific data."}

# @app.get("/merchant_details/{card_holder: str}")
# async def get_merchant_details{card_holder: str}:
# return{"Retrieve detailed information about specific merchants."}




if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app,host="0.0.0.0", port=8000)


# python -m uvicorn main:app --reload