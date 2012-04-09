if (window != top)
    top.location.href = location.href;

var imgs=0;
var jimage, jimagecount, thumbani = 0;
function animateImage()
{
    var src=jimage.attr("src");
    idx=parseInt(src.substr(src.length-1));
    idx=(idx+1)%jimagecount;
    src=src.substr(0,src.length-1) + idx.toString();
    jimage.attr("src", src);
}
function vote(obj,data)
{
    var padre=$(obj).parent();
    padre.find(".vote_up strong").text(data['c'][0]);
    padre.find(".vote_down strong").text(data['c'][1]);
    if($(obj).attr('class').search('vote_up')!=-1)
        padre.removeClass("downactive").addClass("upactive");
    else
        padre.removeClass("upactive").addClass("downactive");
}
function highlight(data,t)
{
    var i=0;
    while(i<t.length && data[i]==t[i])
        i++;

    return data.substr(0,i)+"<strong>"+data.substr(i)+"</strong>"
}
$(document).ready(function()
{
    //autocompletado de busqueda
    var form=$('form[method="get"]').attr("action");
    if($('#q').length)
        $('#q').focus()
        .autocomplete(
        {
            source:form.substr(0,(form.indexOf('?')!=-1)?form.indexOf('?'):form.length)+"/autocomplete?t="+$("#type").val(),
            select:function(event,ui)
            {
                if(ui.item)
                    $('#q').val(ui.item.value);

                $("#submit").click()
            },
            appendTo:($('#search').length)?'#search':'body',
            delay:0,
            minLength: 2
        })
        .data("autocomplete")
        ._renderItem=function(ul,item){return $("<li></li>").data("item.autocomplete",item ).append("<a>"+highlight(item.label,this.term)+"</a>").appendTo(ul)};

    //campos en blanco
    $('.home form,.searchbox form').submit(function()
    {
        $.each($(":input",this), function()
        {
            if(!$(this).val() || $(this).attr("type")=="submit")
                $(this).attr("disabled","disabled")
        })
    })
    .keyup(function(e){if(e.keyCode==13){
        $(":input").removeAttr("disabled")}
    })
    .click(function(){
        $(":input").removeAttr("disabled")
    })
    //mostrar y ocultar aviso idioma
    if(!document.cookie.match("langtest=0"))
        $("#advice").fadeIn();
        
    $(".close_advice").click(function(e)
    {
        document.cookie = "langtest=0";
        $(this).parent().slideUp();
    });
    //buscador index
    $(".home .tabs a").click(function(event)
    {
        event.preventDefault();
        $(".tabs a").removeClass("actual");
        $(this).addClass("actual");
        $("#type").val($(this).attr("id"));
        taming();
    });    
    //thumbnails
    $('.thumblink img').mouseenter(function()
    {
        if (thumbani!=0)
            clearInterval(thumbani);

        jimage=$(this);
        imgs=jimage.attr("class").substr(4).split("_");
        jimagecount=imgs.length;
        thumbani = setInterval(animateImage, 500);
    });
    $('.thumblink img').mouseleave(function()
    {
        if (thumbani!=0)
            clearInterval(thumbani);
    });
    $('.thumblink img').each(function()
    {
        icount = imgs.length;
        src = $(this).attr('src').slice(0,-1);
        for (i=0; i<icount; i++)
            $('<img/>')[0].src = src+i.toString();
    });
    //search    
    $("#advsearch>a").click(function(event)
    {
        event.preventDefault();
        $("#advsearch div").slideToggle();
        if($("span",this).text()=="▶")
            $("span",this).text("▼");
        else
            $("span",this).text("▶")
    });
    if($(location).attr('href').search("size|brate|year")>0)
        $("#advsearch>a").click();
    //download
    $(".download_source input").click(function(){$(this).select()});
    $('.file_comment_vote a').click(function(event)
    {
        if($(this).hasClass("vote_login"))
        {
            event.preventDefault();
            $.ajax({
                dataType:"json",
                url:$(this).attr("href"),
                context:this,
                success:function(data){vote($(this),data)}
            })
        }
    });
    $('.file_download_vote a').click(function(event)
    {
        if($(this).hasClass("vote_login"))
        {
            event.preventDefault();
            $.ajax({
                dataType:"json",
                url:$(this).attr("href"),
                context:this,
                success:function(data){vote(this,data)}
            });
        }
    });
    //translate
    var controls = $('textarea,input[type=text]').not('[name^="safe_"]');
    var empties = controls.filter("[value='']"); 
    $("#searchempty").click(function(event)
    {
        event.preventDefault();
        var offset, docScroll, docHeight;
        docScroll = $("html").scrollTop();
        if(isNaN(docScroll))
            docScroll=0;
        
        $("html").scrollTop(docScroll);
        winHeight = $(window).height();
        empties = controls.filter("[value='']");
        var c;
        for(index = 1; index < empties.length; index++)
        {
            c = $(empties[index]);
            offset=c.offset().top;
            if (offset>docScroll)
            {
                if (offset > winHeight/2) offset -= winHeight/2;
                $("html, body").animate({scrollTop:offset}, 400, function() { c.focus(); });
                break;
            }
        }
        $(this).html(Math.round(10000-10000*empties.size()/controls.size())/100+'% &darr;');
    });
    $("textarea,input[type=text]").change(function()
    {
        empties = controls.filter("[value='']");
        $("#searchempty").html(Math.round(10000-10000*empties.size()/controls.size())/100+'% &darr;');
    });
});
