def print_rangoli(size):
    list_ele = []
    half_str_line = ""
    str_line = ""
    counter= size
    for itr in range(1, size+1):
        alphet= chr(ord('a') + counter - 1)
        str_line = half_str_line+"-" +str(alphet)+"-"+half_str_line[::-1]
        str_line = str_line.strip("-")
        
        counter = counter - 1
        half_str_line = str_line[:itr*2]
        half_str_line=half_str_line.strip("-")
       
    
        list_ele.append(str_line)

    f = (size-1)*2    
    for itr in range(size):
        for itr_dash_1 in range(f):
            print("-",end="")
        print(list_ele[itr],end="")
        for itr_dash_2 in range(f):
            print("-",end="")
        print()
        f=f-2
    g=2
    alpha_2=-2
    for itr in range(size-1):
        for itr_dash_1 in range(g):
            print("-",end="")
        print(list_ele[alpha_2],end="")
        for itr_dash_1 in range(g):
            print("-",end="")
        g=g+2
        alpha_2 = alpha_2-1
        print()

if __name__ == '__main__':
    n = int(input())
    print_rangoli(n)