import numpy as np
import math, sys


class UVDecomposition:
    def __init__(self, input_file, n, m, f, iterations):
        self.inputFile = input_file
        self.N = n
        self.M = m
        self.F = f
        self.U_matrix = None
        self.V_matrix = None
        self.M_matrix = None
        self.M_dash_matrix = None
        self.iterations = iterations
        self.num_rows = 0

    def initialize_matrix(self):
        self.U_matrix = np.ones((self.N, self.F))
        self.V_matrix = np.ones((self.F, self.M))
        self.M_dash_matrix = np.dot(self.U_matrix, self.V_matrix)
        self.M_matrix = np.zeros((self.N, self.M))

        with open(self.inputFile, 'r') as fptr:
            for line in fptr:
                self.num_rows += 1
                [row, col, value] = line.split(',')
                self.M_matrix[int(row) - 1][int(col) - 1] = int(value)

    def update_u_rows(self):
        for r in range(self.U_matrix.shape[0]):
            for s in range(self.U_matrix.shape[1]):
                numerator = 0.0
                denominator = 0.0
                for j in range(self.M):
                    if self.M_matrix[r][j] != 0:
                        k_indices = [l for l in range(self.F) if l != s]
                        UV = 0
                        for k in k_indices:
                            UV += self.U_matrix[r][k] * \
                                  self.V_matrix[k][j]
                        M_UV = self.M_matrix[r][j] - UV
                        numerator += (self.V_matrix[s][j] * M_UV)
                        denominator += (
                        self.V_matrix[s][j] * self.V_matrix[s][j])
                x = float(numerator) / denominator
                self.U_matrix[r][s] = x


    def update_v_columns(self):
        for s in range(self.V_matrix.shape[1]):
            for r in range(self.V_matrix.shape[0]):
                numerator = 0
                denominator = 0
                for i in range(self.U_matrix.shape[0]):
                    if self.M_matrix[i][s] != 0:
                        k_indices = [l for l in range(self.F) if l != r]
                        UV = 0
                        for k in k_indices:
                            UV += self.U_matrix[i][k] * self.V_matrix[k][s]
                        M_UV = self.M_matrix[i][s] - UV
                        numerator += (self.U_matrix[i][r] * M_UV)
                        denominator += (self.U_matrix[i][r] * self.U_matrix[i][r])
                x = float(numerator) / denominator
                self.V_matrix[r][s] = x

    def calculate_rmse(self):
        m_dash_output = np.dot(self.U_matrix, self.V_matrix)
        sum_squared = 0
        for i in range(self.N):
            for j in range(self.M):
                if self.M_matrix[i][j] != 0:
                    sum_squared += (self.M_matrix[i][j] - m_dash_output[i][j]) ** 2
        print("%.4f" % np.sqrt(sum_squared / self.num_rows))




if __name__ == "__main__":
    inputFile = sys.argv[1]
    number_of_users = int(sys.argv[2])
    number_of_products = int(sys.argv[3])
    factors = int(sys.argv[4])
    iterations = int(sys.argv[5])
    UV_obj = UVDecomposition(inputFile,number_of_users,number_of_products,factors,iterations)
    UV_obj.initialize_matrix()
    for itr in range(iterations):
        UV_obj.update_u_rows()
        UV_obj.update_v_columns()
        UV_obj.calculate_rmse()
