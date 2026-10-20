set(SC_TEST_LINK_LIBRARIES sc-cassandra)

add_sc_test(protocol LABELS unit TIMEOUT 30)
add_sc_test(server TIMEOUT 30)
