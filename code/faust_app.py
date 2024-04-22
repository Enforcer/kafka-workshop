from faust import App, Record
import logging


app = App(
    id="example_faust_app",
    broker="kafka://broker:9092",
)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    app.main()
