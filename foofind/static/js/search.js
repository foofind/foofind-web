var cargando,cargando150;
var mas;
var download;
var filtros={};
var pagina=1;
var salida="";
var devueltos=0;
var archivos={};
var peticiones={};
//var ultima=false;
var alto,ancho,arriba,ancho_max_download=1430,ancho_min_download=950,posicion_flecha=612;
function search_string(paginacion)
{
    var filtro="";
    for(var clave in filtros)
        filtro+=clave+"="+filtros[clave]+"&";

    if(paginacion)
        return filtro+"page="+parseInt(pagina);
    else
        return filtro.substr(0,filtro.length-1);
}
//realiza la busqueda con los filtros actuales
function search(paginacion)
{
    $("#more").fadeOut("fast");

    paginacion = (paginacion===true); // el primer parametro puede ser info de un evento
    if(paginacion) 
        pagina++;
    else
    {
        devueltos=0;
        filtros["q"]=$("#q").val();
        if(filtros["q"]=="") return; //si no hay busqueda no hace nada
        window.location.hash="#!/"+search_string();

        if(!cargando)
        {
            cargando=rotar($(".loading"),false);
            cargando150=rotar($("#download>div"),false);
        }
        
        //al cargar la pagina no se borra porque no hay resultados
        if(!$("#results li:first").hasClass("loading"))
        {
            $("#results,#search_info").empty();
            ocultar();
        }
    }
    
    $("#results").append(rotar(cargando,true)).data({result:{total_found:0,time:0}});
    $('#q').autocomplete("disabled");

    parar_peticiones(false);
    peticiones[(paginacion)?"paginacion":"busqueda"]=$.ajax("../search_ajax?"+search_string(paginacion)).done(function(respuesta)
    {
        //si todo esta correcto se muestran los datos y se guardan en los resultados para usarlos luego
        $("#results").data(respuesta);
        cargando=rotar(cargando,false);

        if(respuesta["result"]["total_found"]==0)
            $("#results").html(respuesta["no_results"]);
        else {
            devueltos+=respuesta["files"].length;
            for(resp in respuesta["files"])
                $("#results").append(respuesta["files"][resp]["html"]);
        }
        
        $("#related").empty().append(respuesta["tags"]);
        if(respuesta["result"]["total_found"]>respuesta["result"]["total"])
            $("#results").after($("#more").css("display","block"));
    }).always(function() //no se pone en el done por si falla la peticion ajax
    {
        $('#q').autocomplete("enable");
        var respuesta=$("#results").data();
        if(!paginacion)
            if(respuesta["result"]["total_found"]>0) //si una busqueda realizada y hay resultados se muestra el mensaje
            {
                $("#search_info").html("Más de <strong>"+respuesta["result"]["total_found"]+"</strong> archivos encontrados para <em>"+$("#q").val()+"</em> en "+respuesta["result"]["time"]+" segundos");
            }  
    });
    
    //poner la url de busqueda clásica
    $('header>section>p>a').attr("href","?alt=classic&"+search_string());
}

// actualiza 
function update_search()
{
    // busca el parametro q en la URL
    var q=false;
    params = window.location.href.slice(window.location.href.indexOf('#') + 3).split('&');
    for (param in params)
    {
        var pair = params[param].split("=");
        if (pair[0]=="q"){
            q = pair[1];
            break;
        }
    }
    // si el parametro q ha cambiado, vuelve a buscar
    if (q && q!=filtros["q"]) {
        $('#q').val(q);
        search();
    }
}

function parar_peticiones(paginacion) {
    for(peticion in peticiones)
        if((paginacion || peticion!="paginacion") && peticiones[peticion] && peticiones[peticion].readyState!=4)
            peticiones[peticion].abort();
}

//muestra la ventana de download
function mostrar(file,ocul,e)
{
    if(!$(":animated").length) //si no hay ninguna animacion pendiente
    {
        var id=file.data("id");
        file=file.parents("li");
        alto=$(window).height();
        ancho=$(window).width();
        if(!$("#fondo").data("visible")) //se carga el download si no esta el fondo oscuro
        {
            //si es otro distinto se carga
            if($("#download>div").data("id")!=id)
            {
                if(archivos[id]) //si esta se usa
                    $("#download").html(archivos[id]).addClass("active");
                else //sino se carga
                {
                    $("#download").removeClass("active").html(rotar(cargando150,true));
                    //TODO intentar parar descargas anteriores aqui en lugar del ocultar
                    peticiones["descarga"]=$.get("../download_ajax/"+id).success(function(data)
                    {
                        //sino se hace antes se machaca al hacer el html en download
                        cargando150=rotar(cargando150,false);
                        archivos[id]=data;
                        $("#download").html(data).addClass("active");
                    });
                }
            }
            else //sino solo se indica que esta activo
                $("#download").addClass("active");

            if(ancho>=ancho_max_download) //fondo gris si no estaba active a la derecha
                $("#fondo").data("visible",false);
            else
                $("#fondo").fadeTo(200,.5).data("visible",true);

            //flecha de union
            $("#subcontent").animate({width:ancho-20},{duration:200,queue:false});
            console.log(ancho-ancho_max_download);
            $("#flecha").removeClass("return").css({top:file.position().top+5}).show(100).animate({right:(ancho>ancho_max_download+80)?posicion_flecha+60:(ancho>ancho_max_download)?posicion_flecha+ancho-ancho_max_download-20:(ancho<ancho_min_download)?posicion_flecha-10:posicion_flecha-20},{duration:200,queue:false});
            //quitar otro resultado que estuviera activo y activar el actual
            $("#results li").removeClass("active").css("z-index",0);
            file.addClass("active").css("z-index",5);
            //como esta en position fixed se limita la posicion ya que no se puede hacer por css
            $("#download").css({position:"fixed",top:(e.pageY<alto && arriba<130)?$("#subcontent").position().top-arriba:0,height:alto-65})
            .show(100).animate({right:(ancho>ancho_max_download)?ancho-ancho_max_download:(ancho<ancho_min_download)?ancho-ancho_min_download:0},{duration:200,queue:false});
        }
        else if(ocul)
            ocultar();
    }
}
//oculta la ventana de download
function ocultar(duracion)
{
    if(typeof(duracion) != "number") 
        duracion=200;

    //TODO añadir al if para parar las descargas que no sean la actual
    if($("#download").stop().hasClass("active"))
    {
        //abortar la peticion ajax si se estuviera enviando
        parar_peticiones(true);

        cargando150=rotar(cargando150,false);
    }

    $("#fondo").fadeOut(duracion).data("visible",false);
    //dejar a la derecha de los resultados si no es una busqueda
    if(!$("#results:empty").length) // && ultima==false)// && $("#download").stop().hasClass("active"))
    {
        $("#subcontent").animate({"width":ancho_max_download},{duration:duracion,queue:false});
        $("#flecha").animate({right:posicion_flecha},{duration:duracion,queue:false}).addClass("return"); //necesario por poner el subcontent -20 de ancho
        $("#download").css({position:"fixed",top:(arriba<130)?$("#subcontent").position().top-arriba:0}).animate({right:ancho-ancho_max_download},{duration:duracion,queue:false});
    }
    else //ocultar a la derecha completamente
    {
        //console.log($("#results:empty").length,ultima);
        $("#results li").removeClass("active");
        $("#flecha").hide(duracion).animate({right:-125},{duration:duracion,queue:false});//,complete:function(){$(this).show()}});
        $("#download").hide(duracion).animate({right:-780},{duration:duracion,queue:false,complete:function()
        {
            $(this).removeClass("active").empty();//.show();
        }});        
    }
}
//rota los iconos de carga
function rotar(selector,inicio)
{
    var rotacion=function(rot) //genera el intervalo de rotacion
    {
        return setInterval(function()
        {
            if(Math.random()<0.3)
            {
                r='rotate('+Math.random()*359+'deg)';
                rot.css({'-moz-transform':r,'-webkit-transform':r,'-ms-transform':r,'-o-transform':r,'transform':r})
            }
        },60);
    }
    if(!$(">img",selector).data("rotacion")) //si es la primera vez que se carga se inicializa
        $(">img",selector).data("rotacion",true);

    for(var i=0;i<$(">img",selector).length;i++)
        clearInterval($(">img:eq("+i+")",selector).data("rotacion"));
    
    if(inicio) //si hay que mostrarlo se rota
    {
        for(var i=0;i<$(">img",selector).length;i++)
        {
            var r=$(">img:eq("+i+")",selector);
            r.data("rotacion",rotacion(r))
        }
        return selector.show();
    }
    else //sino se para y se quita
    {
        return selector.hide().detach();
    }
}
$(function()
{
    alto=$(window).height();
    ancho=$(window).width();
    arriba=$(window).scrollTop();
    //extraccion de los filtros de busqueda
    var query=window.location.hash.substring(3).split("&");
    if(query!="")
        for(var values in query)
        {
            pair=query[values].split("=");
            filtros[pair[0]]=pair[1];
        }
    //colocar la informacion y activar botones
    $('#type a:first,#src a:first').addClass("active");
    for(var filtro in filtros)
    {
        switch(filtro)
        {
            case "q":
                $("#q").val(filtros["q"]);
                break;
            case "type":
                $('#type a:first').removeClass("active");
                var tipos=filtros["type"].split("|");
                for(var type in tipos)
                    $("#type a[data-filter="+tipos[type]+"]").addClass("active");

                break;
            case "src":
                $('#src a:first').removeClass("active");
                for(var src in filtros["src"])
                    $("#src a[data-filter^="+filtros["src"][src]+"]").addClass("active");

                break;
            default:
                
        }
    }
    //envio del formulario
    $('.searchbox').submit(function(e)
    {
        search();
        e.preventDefault();
    })
    //filtros
    $('dd').on("click","a",function(e)
    {
        id=$(this).parents("dd").attr("id");
        if($(this).data("filter")=="") //si es el boton de todos
        {
            $(this).toggleClass("active");
            if(filtros[id]) //si tiene filtros los quita todos
            {
                delete(filtros[id]);
                $("#"+id+" li>a:gt(0)").removeClass("active");
            }
            else //sino los pone haciendo clic en cada uno
                $("#"+id+" li>a:gt(0)").click();
        }
        else
        {
            switch(id)
            {
                case "type":
                case "src":
                    if($(this).hasClass("active")) //si esta active se desactiva y se quita el filtro
                    {
                        pipe=((id=="type")?"|":"");
                        filtros[id]=filtros[id].replace($(this).data("filter")+pipe,"").replace(pipe+$(this).data("filter"),"").replace($(this).data("filter"),"");
                        if($("#"+id+" a.active").length==1)
                        {
                            delete(filtros[id]);
                            $("#"+id+" a:first").addClass("active");
                        }
                    }
                    else //sino dependiendo del grupo de filtrado se hace una cosa distinta
                    {
                        if(!filtros[id]) //si no existe se inicializa
                            filtros[id]="";
                        //si es tipo de archivo se concatenan con |
                        filtros[id]+=((id=="type" && filtros[id])?"|":"")+$(this).data("filter");
                        //si es busqueda resrc=swftgelacionada se pone en el formulario
                        if(id=="q")
                            $("#q").val($(this).text());

                        $("#"+id+" a:first").removeClass("active");
                    }
                    $(this).toggleClass("active");
                break;
                default:
                    filtros[id]=$(this).data("filter");
            }        
        }
        search();
        e.preventDefault();
    });
    //ventana de descarga    
    $('#results').on("click","#results>li[id!=no_results]",function(e)
    {
        mostrar($("h3 a",this),true,e);
        e.preventDefault();
        e.stopPropagation();
    }).on("click","a",function(e)
    {
        e.stopPropagation();        
        //si no se usa el Ctrl a la vez que se hace clic o no es el enlace del archivo
        if(!e.ctrlKey && $(this).parent("h3").length)
        {
            e.preventDefault();
            mostrar($(this),true,e);
        }
    })
    /*.on(
    {
        //thumbnails
        hover:(function()
        {
            console.log("cosa");
            if (thumbani!=0)
                clearInterval(thumbani);

            jimage=$(this);
            imgs=jimage.attr("class").substr(4).split("_");
            jimagecount=imgs.length;
            thumbani = setInterval(animateImage, 500);
        },function()
        {
            if (thumbani!=0)
                clearInterval(thumbani);
        }),
        each:function()
        {
            console.log("puta mierda");
            icount = imgs.length;
            src = $(this).attr('src').slice(0,-1);
            for (i=0; i<icount; i++)
                $('<img/>')[0].src = src+i.toString();
        }
    },'li .thumblink img');*/
    //si se desliza la ventana de download es necesaria esta verificacion if(e.pageX<=822 || e.pageX>=1505)
    $("#fondo").hoverIntent({sensitivity:1,over:ocultar,interval:50,out:ocultar,timeout:50});
    $("#flecha").click(function(e){if($("#fondo").data("visible"))ocultar();else if($("+div>div",this).data("id")) mostrar($('h3 a[data-id="'+$("+div>div",this).data("id")+'"]'),false,e)});
    $("#download").on("click","input",function(e){$(this).select();e.stopPropagation()}).hoverIntent(
    {
        sensitivity:1,
        over:function(e){if($(">div",this).data("id"))mostrar($('h3 a[data-id="'+$(">div",this).data("id")+'"]'),false,e)},
        out:function(){},
        interval:50
    });

    if ("size" in filtros)
        size_values = filtros["size"].split(",");
    else
        size_values = [10,34];
    $("#size div").slider(
    {
        min:10,max:34,values:[parseInt(size_values[0]),parseInt(size_values[1])],range:true,
        create:function(e,ui){if(filtros[$(this).attr("id")])$(this).slider("value",filtros[$(this).attr("id")])},
        slide:function(e,ui)
        {
            var size,filesize={},i;
            for(var j in ui.values)
            {                
                size=Math.pow(2,ui.values[j]);
                i=parseInt(Math.floor(Math.log(size)/Math.log(1024)));
                filesize[j]=Math.round(size/Math.pow(1024,i),2)+' '+['Bytes','KiB','MiB','GiB'][i];
            }
            filtros["size"] = [ui.values[0]==10?0:ui.values[0], ui.values[1]==34?50:ui.values[1]];
            $("#size span").eq(0).text(((ui.values[0]==$(this).slider("option","min"))?"< ":"")+filesize[0]);
            $("#size span").eq(1).text(((ui.values[1]==$(this).slider("option","max"))?"> ":"")+filesize[1]);            
        },
        change: search
    });
/*
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
    });*/
    //si se pincha en cualquier parte de los resultados se oculta la ventana de descarga
    $('#results').click(ocultar);
    $('.close_download').on("click", ocultar);
    $(window).scroll(function()
    {
        arriba=$(window).scrollTop();
        //la paginacion continua se activa cuando se baja por los resultados pero antes de llegar al final de la pagina
        if($("#results>li").length==devueltos && arriba+(arriba/1.5)>$(document).height()-alto)
            search(true);

        //oculta la ventana de descarga si no esta cargando y cabe download
        if((!$("#results li:first").hasClass("loading") && ancho>ancho_max_download) || $("#fondo").data("visible"))
            ocultar(100);
        else //sino simplemente se coloca (se añade fixed y right por si estaba en absolute)
            $("#download").css({position:"fixed",top:(arriba<120)?120-arriba:0,right:ancho-ancho_max_download});
            
    }).resize(function()
    {
        alto=$(window).height();
        ancho=$(window).width();
        if($("#fondo").data("visible"))
        {
            $("#subcontent").css({width:ancho});
            $("#flecha").css({right:posicion_flecha});
            $("#download").css({position:"absolute",top:(arriba<50)?0:(arriba>alto)?$("#subcontent").position().top-arriba:0,right:10,height:alto});
            if(ancho>=ancho_max_download) //si la ventana esta desplegada a la derecha
                $("#fondo").fadeOut(200).data("visible",false);
        }
        else
        {//TODO ancho_max_download+20
            $("#subcontent").css({width:ancho-10});
            $("#flecha").css({right:(ancho>ancho_max_download && ancho-ancho_max_download>70)?posicion_flecha+60:(ancho<ancho_min_download)?posicion_flecha-490:ancho-828});
            $("#download").css(
            {
                position:"absolute",
                top:(arriba<50)?0:(arriba>alto)?$("#subcontent").position().top-arriba:0,
                right:(ancho>ancho_max_download)?(ancho-ancho_max_download>70)?70:ancho-ancho_max_download:(ancho<ancho_min_download)?-480:ancho-ancho_max_download,
                height:alto-65
            });
        }
    }); 
    //tambien si se pulsa en ver mas resultados
    $('#more').click(function(e)
    {
        search(true);
        e.preventDefault();
    });
    //hay que ocultar download porque esta fixed y no se puede poner la lista de idiomas encima al estar en absolute
    $('#select_language_box').hover(function()
    {
        $("#download").css("z-index",0);
        ocultar(0);
    },function(){$("#download").css("z-index",6)});
    search();
    setInterval(update_search,250);
});
