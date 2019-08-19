
logging.basicConfig(filename='./model_test.log', datefmt='%m/%d/%Y %I:%M:%S %p',
                    level=logging.DEBUG,
                    format='%(asctime)s %(filename)s: %(message)s',
                    filemode='w')
logging.getLogger("urllib3").propagate = False
# logging.getLogger("requests").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

TRADING_DAYS = 252  # Number of trading days on stock, i.e. time interval of simulation


def _parse_args():
    parser = argparse.ArgumentParser('randomwalk_process',
                                     description='Monte-Carlo simulation of stock prices '
                                                 'behavior based on data from Yahoo')
    parser.add_argument('--start_date', type=str, default='2017-01-01',
                        help="the start date of historical time interval")
    parser.add_argument('--end_date', type=str, default='2019-01-01',
                        help='the end date of historical time interval')
    # parser.add_argument('--number_of_stocks', type=int, default=None,
    #                     help="would be edited, hint: top returns")
    parser.add_argument('--stock_symbols_file', help='path to csv file involving stock symbols')
    parser.add_argument('--stock_symbol', type=str, help="symbol of a stock")
    parser.add_argument('--HTCondor_env', type=int, help="in HTCondor environment or not.")
    return parser.parse_args()


def main():
    logger.info("Starting model/model_test.py")
    start_time = time.time()
    args = _parse_args()
    data_model = DataModel(start_date=args.start_date,
                           end_date=args.end_date,
                           # company_numbers=args.number_of_stocks,
                           companies_symbols_file=args.stock_symbols_file,
                           company_symbol=args.stock_symbol,
                           HTCondor_environment=args.HTCondor_env)
    data_model.run()
    end_time = time.time()
    logger.info("Execution time for simulating {} companies: {}."
                .format(data_model.company_numbers, end_time - start_time))


if __name__ == '__main__':
    # get_all_symbols_and_write()
    main()