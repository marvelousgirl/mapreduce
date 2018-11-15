package com.yq.invertedIndex;

public class test {

    public static void main(String [] args){
        String s = "a b    c \n\n\t5 \t";
        String [] res = s.split("\\s+");
        for(String each: res){
            System.out.println(each);
        }


    }

}


