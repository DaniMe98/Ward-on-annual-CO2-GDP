import numpy as np
from scipy.optimize import curve_fit
import pylab as plt
import pandas as pd

df_new = pd.read_csv('Datasets\co2\CO2 emission by countries.csv', encoding= 'unicode_escape')
df_new= df_new[df_new['CO2 emission (Tons)'] != 0]
df_new=df_new[df_new['Year'] >= 1959]
p=[]
py=[]

for i in range(62):
    py.append(i)

p2=[]
p.append(df_new['CO2 emission (Tons)'].loc[df_new['Country'] == 'Afghanistan'])


px=np.array(p)


for idx in range(62):
    for i in range(12):
        p2.append(px[0][idx])

p1=px[0]


from scipy.stats import linregress
slope,_,_,_,_=linregress(p1, py)
print(slope)
Amplitude=1
Period=2.3
Phase=0
p0 = py

Y=[]
for i in range(0,62):
  Y.append(p0[i] + p1[i]*slope + Amplitude*math.sin(2*math.pi*(slope-Phase)/Period))



N = 62 # number of data points
#lst = sorted(np.random.randn(N))

#print(lst)
t2 = np.linspace(0, 128, N*12)
t = np.linspace(0, 4*np.pi, N)  #create N number from 0 to 4*pi
data = 3.0*np.sin(t+0.001) + 0.5 + Y  # create artificial data with noise

guess_freq = 1
guess_amplitude = 3*np.std(data)/(2**0.5)
guess_phase = 0
guess_offset = np.mean(data)

p0=[guess_freq, guess_amplitude,
    guess_phase, guess_offset]

# create the function we want to fit
def my_sin(x, freq, amplitude, phase, offset):
    return np.sin(x * freq + phase) * amplitude + offset 

# now do the fit
fit = curve_fit(my_sin,t, data , p0=p0)

# we'll use this to plot our first estimate. This might already be good enough for you
#t2 = np.linspace(0, 4*np.pi, N)
data_first_guess = my_sin(t2, *p0)

# recreate the fitted curve using the optimized parameters
data_fit = my_sin(t, *fit[0])

plt.plot(data,'.')
#plt.plot(np.sin(t)+data, '.')
plt.plot(data_fit, label='after fitting')
#plt.plot(data_first_guess, label='first guess')
plt.legend()
plt.show()

