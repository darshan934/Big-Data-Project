with open('/Users/hka/Downloads/reviews_Books_5.json', 'r', encoding = 'utf-8') as f:
    count = 50000

    with open('/Users/hka/Downloads/test50000.json', 'w') as p:

        for line in f:
            p.write(line)
            count -= 1
            if count < 0:
                break
    # print(count)
