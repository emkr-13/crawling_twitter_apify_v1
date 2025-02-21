if __name__ == "__main__":
    import uvicorn
    from decouple import config

    uvicorn.run(
        "src.main:api", port=int(config("APP_PORT"))
    )