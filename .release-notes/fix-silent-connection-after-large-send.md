## Fix connection hang after sending a large amount of data

After sending a large query or a large amount of data, a connection could stop receiving the server's responses — an in-flight query would never complete and the connection would hang. No error was raised and the connection was not closed; it simply went quiet, even though the server had already replied. This most often showed up with large statements or bulk data. Responses now arrive as expected.
