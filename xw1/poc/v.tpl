<html>
<h3> Metric Values for requested coordinates (Lat/Long)  {{pos}} .</h3>
<b>Station ID : </b>{{station_id}}
<b> Distance from coordinates :</b> {{distance}} kms 
<br>
<hr>
<table>
<b>Other Nearby Stations (Within 30 Kms) :</b>
%for item in all_stations[1:]:
   <tr>
   <td> {{item[0]}} : </td>
   <td> {{item[1]}} kms </td>
   </tr>
%end
</table>
<hr>
<table>
  %for item in post:
    <tr>
   <td> <b>{{item[0]}} :</b></td>
   <td> {{item[1]}} </td> 
   <td> <i>  {{item[2]}} </i></td>
    </tr>
  %end
</table>

</html>
