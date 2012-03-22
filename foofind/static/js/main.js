$(function(){
	$("#q").change(function(){
		$.ajax({
			url: "/search/"+$(this).val(),
			dataType: "html",
			success: function(data){
				$("#results").html(data);
			}
		});
	});
});
