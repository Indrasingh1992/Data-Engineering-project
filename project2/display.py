# function for bubble sorting
def bubble_sort(arr):
    n = len(arr)
    for i in range(n):
        for j in range(0, n-i-1):
            if arr[j] > arr[j+1]:
                arr[j], arr[j+1] = arr[j+1], arr[j]
                print("Pass", i+1, ":", arr)
                print("Swapped", arr[j], "and", arr[j+1])
                print()


arr= [5,4,3,2,1]
print(bubble_sort(arr))
