from fastapi import FastAPI, HTTPException


app = FastAPI()

deliverymen_data = [
    {"_id": "00ga56cqwcxm789920ft8siqr", "name": "Екатерина Великая"},
    {"_id": "68ga56cqwcxm79920ft8lkjhg", "name": "Дора Величковская"}
]

deliveries_data = [
    {
        "order_id": "772738ba5b1241c78abc7e17",
        "order_date_created": "2024-12-04 12:50:27.43000",
        "delivery_id": "6222053d10v01cqw379td2t8",
        "deliveryman_id": "68ga56cqwcxm79920ft8lkjhg",
        "delivery_address": "Ул. Мира, 7, корпус 1, кв. 4",
        "delivery_time": "2024-12-04 13:11:23.621000",
        "rating": 5,
        "tips": 500
    }
]


@app.get("/get_data")
async def get_data(table_name):
    if table_name == "deliveryman":
        return deliverymen_data
    elif table_name == "delivery":
        return deliveries_data
    else:
        raise HTTPException(status_code=404, detail="Table not found")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
