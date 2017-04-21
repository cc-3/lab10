"""
  _         _    _  __
 | |   __ _| |__/ |/  \.
 | |__/ _` | '_ \ | () |
 |____\__,_|_.__/_|\__/

  ___ ___ ___   ___                _
 | _ ) __/ __| / __|_ __  __ _ _ _| |__
 | _ \ _|\__ \ \__ \ '_ \/ _` | '_| / /
 |___/_| |___/ |___/ .__/\__,_|_| |_\_\.
                   |_|
"""

import Sliding
import argparse

from pyspark import SparkConf
from pyspark import SparkContext


"""
    AQUI DEFINAN SUS FUNCIONES MAP Y REDUCE, HELPER FUNCTIONS ETC

    NOTA: NO TRATEN DE HACER TODO EN UNA SOLA FUNCION, ESO ES UNA BAD IDEA.
"""


def bfs_reduce(v1, v2):
    pass


def bfs_map(pair):
    pass


def solve_sliding_puzzle(master, output, height, width):
    """
        Aqui tienen que construir el RDD Spark y todo lo demas

        @param master: el master que tienen que utilizar
        @param output: la funcion que espera un string para escribir en archivo
        @param height: el height
        @param width: el width
    """
    global w,h,level

    level = 0
    w = width
    h = height

    #solucion
    solution = Sliding.solution(w,h)
    # Conf
    conf = SparkConf().setAppName('SlidingBFS').setMaster(master)
    # Spark Context
    sc = SparkContext(conf=conf)

    #######################################
    #  AQUI TIENEN QUE PONER SU SOLUCIÓN  #
    #######################################

    sc.stop()

"""
    ---------------------------------------------------------------------
    NO MODIFIQUEN NADA A PARTIR DE AQUI

    Pueden leer el codigo y tratar de entenderlo, pero no es necesario
    que se preocupen de entenderlo.
"""


def main():
    """
        Parsea los argumentos del command line y corre el solver.
        Si ningun argumento es pasado se corre utilizando los valores
        por defecto.
    """
    parser = argparse.ArgumentParser(
            description="Regresa la solucion entera del grafo.")
    parser.add_argument("-M", "--master", type=str, default="local[8]",
                        help="url del master para este trabajo")
    parser.add_argument("-O", "--output", type=str, default="solution-out",
                        help="nnombre del output file")
    parser.add_argument("-H", "--height", type=int, default=2,
                        help="height del puzzle")
    parser.add_argument("-W", "--width", type=int, default=2,
                        help="width del puzzle")
    args = parser.parse_args()

    # Abre el archivo donde van a escribir la solucion y define
    # Una funcion para escribir en el
    output_file = open(args.output, "w")

    def myWriter(line):
        output_file.write(line + '\n')  # output_file es un upvalue

    writer = myWriter

    # Llama a su solver
    solve_sliding_puzzle(
        args.master,
        writer,
        args.height,
        args.width,
    )

    # cierra el output file
    output_file.close()

# Ejecuta este archivo si se corre directamente
if __name__ == "__main__":
    main()
