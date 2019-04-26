

DataFrame1.join(DataFrame2, DataFrame1.col("<column_name>").equalTo(DataFrame2("<column_name2>")))


// Joining DataFrame one after another

DataFrame1.unionAll(DataFrame2)