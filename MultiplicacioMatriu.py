from cos_backend import COSBackend
import pywren_ibm_cloud as pywren
import numpy as np
import random 
import pickle
import time


m=50
n=50
l=50
a=25

mImpar=m%a
lImpar=l%a
nWorkersA=int(m/a) 
nWorkersB=int(l/a)
if(mImpar!=0):
    nWorkersA+=1
if(lImpar!=0):
    nWorkersB+=1
w=nWorkersA*nWorkersB

 
def matrix_multiplication(data):
    cos=COSBackend()
    valuesWorker=pickle.loads(cos.get_object('practica-sd-mp',f'{data}'))
    worker=data.split("w")
    i=int(worker[0])
    j=int(worker[1])

    #ara que tenim les files i columnes a calcular les calculem
    resultats=[]
    for lineA in valuesWorker[0]:
        resultatsFila=[]
        for columnB in valuesWorker[1]:
            total=0
            for x in range(n):
                total+=lineA[x]*columnB[x]
            resultatsFila.append(total)
        resultats.append(resultatsFila)
    return resultats

def multiplication_reduce(results):
    cos=COSBackend()
    matrixC=[]

    #quan acabi aquest for ja haurem tractat tots els casos
    for indexWorkerFila in range(nWorkersA):
        for numeroFila in range(len(results[indexWorkerFila*nWorkersB])):
            fila=[]
            for indexWorkerColumna in range(nWorkersB):
                contadorWorker=indexWorkerFila*nWorkersB+indexWorkerColumna
                for valor in results[contadorWorker][numeroFila]:
                    fila.append(valor)
            matrixC.append(fila)
    cos.put_object('practica-sd-mp','matrixC.txt', pickle.dumps(matrixC))


def generatex(x,y,z,a):
    cos=COSBackend()
    matrixA=[]
    matrixB=[]
    for m_value in range(x):
        valors=[]
        for n_value in range(y):
            valors.append(random.randint(0,10))
        matrixA.append(valors)
    for n_value in range(y):
        valors=[]
        for l_value in range(z):
            valors.append(random.randint(0,10))
        matrixB.append(valors)
    cos.put_object('practica-sd-mp', 'matrixA.txt', pickle.dumps(matrixA))
    cos.put_object('practica-sd-mp', 'matrixB.txt', pickle.dumps(matrixB))

    for i in range(nWorkersA):
        if(mImpar!=0 and i==nWorkersA-1):
            filesA=matrixA[i*a:]
        else: filesA=matrixA[i*a:i*a+a]
        for j in range(nWorkersB):
            columnesB=[]
            if(lImpar!=0 and j==nWorkersB-1):
                columnesTotals=lImpar
            else: columnesTotals=a
            for k in range(columnesTotals):
                columna=[item[j*a+k] for item in matrixB]
                columnesB.append(columna)
            #ja tinc les files i les columnes
            infoWorkers=[]
            infoWorkers.append(filesA)
            infoWorkers.append(columnesB)
            cos.put_object('practica-sd-mp', f'{i}w{j}', pickle.dumps(infoWorkers))

if __name__ == '__main__':
    cos=COSBackend()
    ibcmf= pywren.ibm_cf_executor()
    start_time = time.time()
    ibcmf.wait(ibcmf.call_async(generatex,[m,n,l,a]))
    ibcmf.clean()
    iterdata=[]
    
    for i in range(nWorkersA):
        for j in range(nWorkersB):
            iterdata.append(f'{i}w{j}')
    #start_time = time.time()
    ibcmf.wait(ibcmf.map_reduce(matrix_multiplication,iterdata, multiplication_reduce, reducer_wait_local=True))
    elapsed_time = time.time() - start_time
    for i in iterdata:
        cos.delete_object('practica-sd-mp',i)


    matrixA=pickle.loads(cos.get_object('practica-sd-mp','matrixA.txt'))
    matrixB=pickle.loads(cos.get_object('practica-sd-mp','matrixB.txt'))
    matrixC=pickle.loads(cos.get_object('practica-sd-mp','matrixC.txt'))
    print(f'Matriu A ({m} x {n}):')
    for filaA in matrixA:
        print(filaA)
    print(f'Matriu B ({n} x {l}):')
    for filaB in matrixB:
        print(filaB)
    print(f'Matriu C ({m} x {l}):' )
    for filaC in matrixC:
        print(filaC)
    print(f'Valor de m: {m}\nValor de n: {n}\nValor de l: {l}\nValor de a: {a}')
    print(f'El número total de workers ha sigut de: {w}.\nTemps que ha passat en segons: {elapsed_time} s')