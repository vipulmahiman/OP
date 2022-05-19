import pytest
from pyspark.sql import functions as F
from pyspark.sql.session import SparkSession
from pyspark_test import assert_pyspark_df_equal

import datetime

from src.op_ingestion import validate_file, calculate_delta, apply_delta

index = ["id", "activation_date", "active", "owner_id", "billing_date", "billing_tax_number", "company_name",
             "company_email", "contact_name", "is_billing"]

# @pytest.fixture(scope="session")
# def spark_test_session():
#     return (
#         SparkSession
#             .builder
#             .master('local[*]')
#             .appName('OP-Test-unit-testing')
#             .getOrCreate()
#     )
#

def test_validate_file(spark_test_session, mocker):
    """
    Test the count function with condition
    :param spark_test_session:
    :return:
    """
    sc = spark_test_session

    #1. test for various format issues
    test_error_data = [(5090908953, datetime.date(2021, 10, 27), 3, 349941983, datetime.date(2021, 11, 15),
                        '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                        0),
                       (5090908954, datetime.date(2021, 10, 27), 0, 349941983, datetime.date(2021, 11, 15),
                        '84.307.791.8+123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                        0),
                       (5090908955, datetime.date(2021, 10, 27), 0, 349941983, datetime.date(2021, 11, 15),
                        '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelindaud.go.id', 'Dr. Virman Wahyuni',
                        0),
                       (5090908956, datetime.date(2021, 10, 27), 0, 349941983, datetime.date(2021, 11, 15),
                        '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                        0),
                       (5090908957, datetime.date(2021, 10, 27), 0, 349941983, datetime.date(2021, 11, 15),
                        '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                        0)
                       ]
    test_error_data_df = sc.createDataFrame(test_error_data, index)
    mocker.patch('src.op_ingestion.file_loader', return_value=test_error_data_df)
    out_error_list = validate_file(test_error_data_df)
    # print(out_error_list)
    expected_error = ["5090908953: {'active': ['max value is 1']}",
                      '5090908954: {\'billing_tax_number\': ["value does not match regex \'^[0-9]{2}.[0-9]{3}.[0-9]{3}.[0-9]{1}-[0-9]{3}.[0-9]{3}$\'"]}',
                      '5090908955: {\'company_email\': ["value does not match regex \'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\\\.[a-zA-Z0-9-.]+$\'"]}']
    assert out_error_list == expected_error

    #2. test for correct format data
    test_correct_data = [(5090908953, datetime.date(2021, 10, 27), 0, 349941983, datetime.date(2021, 11, 15),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0),
                      (5090908954, datetime.date(2021, 10, 27), 0, 349941983, datetime.date(2021, 11, 15),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0),
                      (5090908955, datetime.date(2021, 10, 27), 0, 349941983, datetime.date(2021, 11, 15),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0),
                      (5090908956, datetime.date(2021, 10, 27), 0, 349941983, datetime.date(2021, 11, 15),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0),
                      (5090908957, datetime.date(2021, 10, 27), 0, 349941983, datetime.date(2021, 11, 15),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0)
                      ]
    test_correct_data_df = sc.createDataFrame(test_correct_data, index)
    mocker.patch('src.op_ingestion.file_loader', return_value=test_correct_data_df)
    out_error_list = validate_file(test_correct_data_df)
    expected_error = []
    assert out_error_list == expected_error


def test_calculate_delta(spark_test_session):
    sc = spark_test_session
    test_prev_data = [(5090908953, datetime.date(2021, 10, 27), 0, 349941983, datetime.date(2021, 11, 15),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0),
                      (5090908954, datetime.date(2021, 10, 28), 0, 349941983, datetime.date(2021, 11, 15),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0),
                      (5090908955, datetime.date(2021, 10, 29), 0, 349941983, datetime.date(2021, 11, 15),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0),
                      (5090908956, datetime.date(2021, 10, 30), 0, 349941983, datetime.date(2021, 11, 15),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0),
                      (5090908957, datetime.date(2021, 10, 31), 0, 349941983, datetime.date(2021, 11, 15),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0)
                      ]

    test_curr_data = [(5090908953, datetime.date(2021, 10, 27), 0, 349941985, datetime.date(2021, 11, 16),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0),
                      (5090908954, datetime.date(2021, 10, 28), 0, 349941983, datetime.date(2021, 11, 15),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0),
                      (5090908955, datetime.date(2021, 10, 29), 0, 349941983, datetime.date(2021, 11, 15),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0),
                      (5090908956, datetime.date(2021, 10, 30), 0, 349941983, datetime.date(2021, 11, 15),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0),
                      (5090908960, datetime.date(2021, 10, 31), 0, 349941983, datetime.date(2021, 11, 15),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0)
                      ]
    test_df_prev = sc.createDataFrame(test_prev_data, index)
    test_df_curr = sc.createDataFrame(test_curr_data, index)
    df_delta_output = calculate_delta(test_df_prev, test_df_curr, "")
    expected_output_data = [(5090908960, datetime.date(2021, 10, 31), 0, 349941983, datetime.date(2021, 11, 15),
                          '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id',
                          'Dr. Virman Wahyuni',
                          0, [""], 'I'),
                         (5090908953, datetime.date(2021, 10, 27), 0, 349941985, datetime.date(2021, 11, 16),
                          '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id',
                          'Dr. Virman Wahyuni',
                          0, ['owner_id', 'billing_date'], 'M'),
                         (5090908957, datetime.date(2021, 10, 31), 0, 349941983, datetime.date(2021, 11, 15),
                          '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id',
                          'Dr. Virman Wahyuni',
                          0, [""], 'D')]
    expected_output_index = [*index,'column_names','row_status']
    df_expected_output = sc.createDataFrame(expected_output_data, expected_output_index)
    print(df_delta_output.show())
    print(df_expected_output.show())
    df_delta_output = df_delta_output.drop("ingestion_date")
    assert_pyspark_df_equal( df_delta_output , df_expected_output)

def test_apply_delta(spark_test_session):
    sc = spark_test_session
    test_prev_data = [(5090908953, datetime.date(2021, 10, 27), 0, 349941983, datetime.date(2021, 11, 15),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0),
                      (5090908954, datetime.date(2021, 10, 28), 0, 349941983, datetime.date(2021, 11, 15),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0),
                      (5090908955, datetime.date(2021, 10, 29), 0, 349941983, datetime.date(2021, 11, 15),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0),
                      (5090908956, datetime.date(2021, 10, 30), 0, 349941983, datetime.date(2021, 11, 15),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0),
                      (5090908957, datetime.date(2021, 10, 31), 0, 349941983, datetime.date(2021, 11, 15),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0)
                      ]
    expected_curr_data = [(5090908953, datetime.date(2021, 10, 27), 0, 349941985, datetime.date(2021, 11, 16),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0),
                      (5090908954, datetime.date(2021, 10, 28), 0, 349941983, datetime.date(2021, 11, 15),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0),
                      (5090908955, datetime.date(2021, 10, 29), 0, 349941983, datetime.date(2021, 11, 15),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0),
                      (5090908956, datetime.date(2021, 10, 30), 0, 349941983, datetime.date(2021, 11, 15),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0),
                      (5090908960, datetime.date(2021, 10, 31), 0, 349941983, datetime.date(2021, 11, 15),
                       '84.307.791.8-123.000', 'CV Habibi Sudiati', 'hidayantobelinda@ud.go.id', 'Dr. Virman Wahyuni',
                       0)
                      ]
    test_df_prev = sc.createDataFrame(test_prev_data, index)
    df_expected_curr = sc.createDataFrame(expected_curr_data, index)
    df_delta_output = calculate_delta(test_df_prev, df_expected_curr, datetime.datetime.now())
    df_output_curr = apply_delta(test_df_prev, df_delta_output)
    print("Printing new output dataset")
    print(df_output_curr.show())
    print("Printing Expected dataset")
    print(df_expected_curr.show())

    df_output_curr = df_output_curr.orderBy('id')
    df_expected_curr = df_expected_curr.orderBy('id')

    assert_pyspark_df_equal(df_output_curr, df_expected_curr)

