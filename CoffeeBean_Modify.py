import pandas as pd


#1)커피빈 매장 시각화 : 파일 다운로드
CB = pd.read_csv('D:/Crawling_data/CoffeeBean.csv', encoding='CP949', index_col=0, header=0, engine='python')
#print(CB.head())


#store_name은 매장의 이름만을 저장한 배열이다.
store_name=[]
for store in CB.store:
       store_name.append(str(store))    #이 작업을 매장 갯수만큼 반복
#print(store_name[:5])


#주의!!! --->  str(address).split()이렇게하면 데이터 프레임 형성이 안되니까 반드시 .split()을 없애고 데이터 프레임 만들기!

#행정구역 이름 보정하기
# addr은 매장의 주소만을 저장한 배열이다.
addr = []
for address in CB.address:
       addr.append(str(address).split())    #이 작업을 매장 갯수만큼 반복
#print(addr[:5])

       
# plan : step1_시도, step2_군구로 나눈다.


#step1_시도

#시도를 district.csv에서의 이름으로 고친다.
#0번 열이 시도 열이니까 0번 열을 정확하게 시도를 표시해준 이름으로 교정해준다.
addr2 = []
for i in range(len(addr)):
    if addr[i][0] == "서울": addr[i][0]="서울특별시"
    elif addr[i][0] == "서울시": addr[i][0]="서울특별시"
    elif addr[i][0] == "부산시": addr[i][0]="부산광역시"
    elif addr[i][0] == "인천": addr[i][0]="인천광역시"
    elif addr[i][0] == "광주": addr[i][0]="광주광역시"
    elif addr[i][0] == "대전시": addr[i][0]="대전광역시"
    elif addr[i][0] == "울산시": addr[i][0]="울산광역시"    
    elif addr[i][0] == "세종시": addr[i][0]="세종특별자치시"
    elif addr[i][0] == "경기": addr[i][0]="경기도"
    elif addr[i][0] == "충북": addr[i][0]="충청북도"
    elif addr[i][0] == "충남": addr[i][0]="충청남도"
    elif addr[i][0] == "전북": addr[i][0]="전라북도"
    elif addr[i][0] == "전남": addr[i][0]="전라남도"
    elif addr[i][0] == "경북": addr[i][0]="경상북도"
    elif addr[i][0] == "경남": addr[i][0]="경상남도"
    elif addr[i][0] == "제주": addr[i][0]="제주특별자치도"
    elif addr[i][0] == "제주도": addr[i][0]="제주특별자치도"
    elif addr[i][0] == "제주시": addr[i][0]="제주특별자치도"                             
    addr2.append(' '.join(addr[i]))
#print(addr2[:5])


#addr배열에서 시도 군구를 split하여 배열로 저장한다.
addr3=[]
for address in addr2:
       addr3.append(str(address).split())    #이 작업을 매장 갯수만큼 반복
#print(addr3[:5])

"""[['서울특별시', '강남구', '학동로', '211', '1층'], ['서울특별시', '강남구', '광평로', '280', '수서동', '724호'],
 ['서울특별시', '강남구', '논현로', '566', '강남차병원1층'], ['서울특별시', '강남구', '테헤란로152', '강남파이낸스빌딩1층10-A호'],
 ['서울특별시', '서초구', '강남대로', '369', '1층']]"""

#각 배열에서 첫 번째 원소만을 추출하여 시도로 저장하면된다.

sido=[]
for i in range(len(addr3)):
       sido.append(addr3[i][0])
#print(sido)

gungu=[]
for i in range(len(addr3)):
       gungu.append(addr3[i][1])
#print(gungu)




#매장명 데이터프레임에 sido데이터프레임과 gungu데이터프레임 붙이기
storeName = pd.DataFrame(store_name, columns=['store_name'])
#print(storeName)

sido = pd.DataFrame(sido, columns=['sido'])
gungu = pd.DataFrame(gungu, columns=['gungu'])


df1=pd.concat([storeName,sido],axis=1)
df2=pd.concat([df1,gungu],axis=1)
#print(df2)

"""       store_name   sido gungu
0         학동역 DT점  서울특별시   강남구
1             수서점  서울특별시   강남구
2            차병원점  서울특별시   강남구
3           스타타워점  서울특별시   강남구
4           강남대로점  서울특별시   서초구
..            ...    ...   ...
290       창원시티세븐점   경상남도   창원시
291        종로구청앞점  서울특별시   종로구
292          상수역점  서울특별시   마포구
293        안양시청앞점    경기도   안양시
294  하남신세계백화점 B1점    경기도   하남시

[295 rows x 3 columns]"""



#2-2) 커피빈 주소 데이터 보정 
#군구정보를 정정하기위한 행정구역 데이터(distric.csv)를 이용하여 행정구역 데이터 보정.- 주소 데이터에서 잘못된 행정구역을 찾아서 보정


"""행정구역 데이터(distric.csv)를 이용하여 행정구역 데이터 보정
#df2에 있는 gungu와 distrivc.csv에 있는 gungu를 비교하면서 보정.
Sido table에는 없는 데이터를 찾으면 된다. Table.merge()로 찾는다.
판다스 테이블로 찾으면 데이터프레임이라 머지 함수 쓸 수 있다."""

sido_table = pd.read_csv("D:/Crawling_data/district.csv", encoding='CP949', index_col=0, header=0,engine='python' )

m = df2.merge(sido_table, on=['sido', 'gungu'], how='outer', suffixes=['', '_'], indicator=True)

m_result = m.query('_merge=="left_only" ')

m_result[['sido','gungu']]
#print(m_result)


"""    store_name     sido gungu     _merge
246   고대세종캠퍼스점  세종특별자치시   세종로  left_only"""


gungu_alias= """ 세종로:세종시 """

gungu_dict = dict(aliasset.split(':') for aliasset in gungu_alias.split())

df2.gungu = df2.gungu.apply(lambda v: gungu_dict.get(v,v))

m = df2.merge(sido_table, on= ['sido', 'gungu'], how='outer', suffixes=['', '_'], indicator=True)

m_result = m.query('_merge =="left_only"')

m_result[['sido', 'gungu']]

#print(m_result[['sido', 'gungu']])

print(df2)


#3) 결과 테이블을 파일로 저장.
m.to_csv('D:/Crawling_data/CB_2.csv', encoding='CP949', mode='w', index=True)



#CoffeeBean으로 매장 위치 시각화하기
CB_geoData = pd.read_csv('D:/Crawling_data/user/CB_geo.shp.csv', encoding='cp949',  engine='python')


import folium
map_store = folium.Map(location=[37.560284, 126.975334], zoom_start = 12)


#매장 갯수만큼 반복해준다.
#iterrows()를 쓰면 행 단위로 반복을 한다.
#store는 현재 처리하고 있는 매장 하나에 대한 정보
#popup은 매장 마커에 같이 표시해줄 정보다.
#.addto(map_store)는 구한 정보를 저장해준다.
for i, store in CB_geoData.iterrows():   
    folium.Marker(location=[store['위도'], store['경도']], popup=store['store']+store['phone'], icon=folium.Icon(color='red', icon='star')).add_to(map_store)
#객체 저장. 객체 이름은 map_store_1
map_store.save('D:/Crawling_Num2/map_store_1.html')


# 서울 행정구역 경계선 표시 추가하기. 
import  json
#jsonb.loads는 json 형태로 그대로 읽어오는 역할을 함
with open('D:/Crawling_data/user/seoul_municipalities_geo.json', mode='rt', encoding='utf-8') as f:
    seoul = json.loads(f.read())
    f.close()

#지도객체에 json파일을 적용시켜서 읽어준다.
#서울 행정구역 정보가 들어있는 지도 객체에 json 파일을 읽어서 저장해준다.
folium.GeoJson(
    seoul,
    name='seoul_municipalities'
).add_to(map_store)
map_store.save('D:/Crawling_Num2/map_store_2.html')



