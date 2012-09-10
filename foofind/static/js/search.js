var cargando,cargando150;
var filtros;
var pagina=1; //necesario ponerlo tambien aqui para que funcione la paginacion de la primera busqueda
var devueltos=0;
var archivos={};
var peticiones={};
var alto,ancho,arriba,container,ancho_max_download=1430,ancho_min_download=950,posicion_flecha=612; //TODO min-width 940
var respuesta_total_found=-1;
var ultima_busqueda="";

//obtiene la cadena de busqueda con o sin paginación
function search_string(paginacion, download)
{
    var filtro="";
    for(var clave in filtros)
        if(download!==false || clave!="d")
            filtro+=clave+"="+(clave=="q"?encodeURIComponent(filtros[clave]).replace(/%20/g,"+"):filtros[clave])+"&";

    if(paginacion)
        return filtro+"page="+parseInt(pagina);
    else
        return filtro.substr(0,filtro.length-1);
}
//extraccion de los filtros de busqueda y actualizacion de la interfaz
function search_params()
{
    filtros={};
    var query=window.location.hash?window.location.hash.substring(3).split("&"):window.location.search.substring(1).split("&");
    if(query!="")
        for(var values in query)
        {
            pair=query[values].split("=");
            filtros[pair[0]]=decodeURIComponent(pair[1]).replace(/\+/g," ");
        }

    //colocar la informacion y activar botones
    $('#src a, #type a').removeClass("active");
    if("q" in filtros)
        $("#q").val(filtros["q"]);

    if("type" in filtros)
    {
        var tipos=filtros["type"].split("|");
        for(var type in tipos)
            $("#type a[data-filter="+tipos[type]+"]").addClass("active");
    }
    else
        $('#type a:first').addClass("active");

    if("src" in filtros)
        for(var src in filtros["src"])
            $("#src a[data-filter^="+filtros["src"][src]+"]").addClass("active");
    else
        $('#src a:first').addClass("active");

    $("#size div").slider("values","size" in filtros?filtros["size"].split(","):[17,34]);
}

//realiza la busqueda con los filtros actuales
function search(paginacion)
{
    $('#q').blur().autocomplete({disabled:true}); //si no pierde el foco no se desactiva
    $("#more").fadeOut("fast");
    paginacion=(paginacion===true); //el primer parametro puede ser info de un evento
    if(paginacion)
        pagina++;

    var search_string_filters=search_string(paginacion,false);
    var search_s=!cargando?search_string(paginacion):search_string_filters; //si es la primera vez tiene que coger el parametro d
    if(!paginacion)
    {
        if(ultima_busqueda==search_string_filters) //no busca si la busqueda no ha cambiado
            return;

        parar_peticiones(true);
        ultima_busqueda=search_string_filters;
        pagina=1;
        devueltos=0;
        //poner la url de busqueda, la de busqueda clásica y la de cambio de idioma
        if(window.location.hash)
            window.location.hash="#!/"+search_s;

        $('header>section>p>a').attr("href","?alt=classic&"+search_s);
        $('#select_language_box a[href]').not(":last").each(function(){$(this).attr("href","/"+$(this).data("lang")+"/setlang?"+search_s)});
        if(!cargando) //inicializar los ojitos
        {
            cargando=rotar($(".loading"),false);
            cargando150=rotar($("#download>div"),false);
        }
        else //al cargar la pagina no se borra porque no hay resultados
        {
            $("#results,#search_info").empty();
            ocultar();
        }
    }
    if(filtros["q"]=="") //si no hay busqueda
        $("#q").attr("placeholder",$("#search_submit").data("no_query")).blur();
    else //se ponen ojitos, se paran peticiones antiguas y se realiza la nueva
    {
        var results = $("#results");
        results.append(rotar(cargando,true)).data({result:{total_found:0,time:0}});
        peticiones[paginacion?"paginacion":"busqueda"]=$.ajax("../search_ajax?"+search_s).done(function(respuesta)
        {
            if(respuesta)
            {
                cargando=rotar(cargando,false); //parar ojitos
                for(var resp in respuesta["files_ids"]) //añadir resultado si no estaba ya antes
                {
                    devueltos++;
                    if(!results.children('a[data-id="'+respuesta["files_ids"][resp]+'"]').length)
                    {
                        results.append(respuesta["files"][resp])
                        //precarga de miniaturas
                        var result=$(respuesta["files"][resp]).find(".thumblink img");
                        if(result.data("servers"))
                            for(var index in result.data("servers").split("_"))
                                $('<img>').attr("src",result.attr('src').slice(0,-1)+index);
                    }
                }
                if(!paginacion) //si no es paginacion
                    if(respuesta["total_found"]==0) //si no hay resultados
                        results.html(respuesta["no_results"]);
                    else //si hay resultados se muestra el mensaje resumen
                        $("#search_info").html(respuesta["result_number"]);

                //$("#related").empty().append(respuesta["tags"]); de momento no hay tags
                respuesta_total_found = respuesta["total_found"];
                more_results=load_more_results();
                if(more_results<0 && $("#results li:first").hasClass("loading")) //si es la ultima pagina posible y no hay resultados
                    results.html(respuesta["no_results"]);
                if(more_results==2)
                    results.after($("#more").css("display","block"));

                if("d" in filtros) //si en la URL venia un archivo se intenta mostrar directamente
                    mostrar($('#results h3 a[data-id="'+filtros["d"]+'"]'));
            }
            else //si hay un error en el servidor se vuelve a pedir
                search(paginacion);

        }).fail(function(respuesta){results.html(respuesta["no_results"])});
    }
    $('#q').autocomplete({disabled:false});
}

function load_more_results()
{
    if (pagina<respuesta_total_found/10) //si hay mas paginas para cargar
    {
        //si ya estan puestos todos los resultados que se han devuelto y la altura de los mismos es menor de lo esperado se busca
        if($("#results>li").length==devueltos && arriba>$("#results").height()-104*(arriba==0?3:5)-alto)
        {
            search(true);
            $("#more").css("display","none");
            return 1;
        }
        else
            return 2;
    }
    return -1;
}

//detiene las peticiones ajax de busqueda, archivo o paginacion
function parar_peticiones(paginacion)
{
    for(peticion in peticiones)
        if((paginacion || peticion!="paginacion") && peticiones[peticion] && peticiones[peticion].readyState!=4)
            peticiones[peticion].abort();
}
//muestra la ventana de download
function mostrar(file,ocul)
{
    //para corregir el error cuando se llama desde el hoverIntent de download y no hay li.active o si hay alguna animacion pendiente
    if(!file.length || $(":animated").length)
        return;

    var download=$("#download");
    var id=file.data("id");
    file=file.parents("li");
    alto=$(window).height();
    ancho=$(window).width();
    filtros["d"] = id;
    if(window.location.hash)
        window.location.hash="#!/"+search_string(false);
    else
        window.location.search=search_string(false);

    if(!$("#fondo").data("visible")) //se carga el download si no esta el fondo oscuro
    {
        if($(">article",download).data("id")!=id) //si es otro distinto se carga
        {
            if(archivos[id]) //si esta se usa
            {
                var play=$(".thumblink",file);
                if(play.data("play"))
                    if(play.data("active_play")) //si se ha pulsado en el link de previsualizacion se activa en el embed
                    {
                        play.data("active_play",false);
                        download.html(archivos[id].replace(play.data("play")[0],play.data("play")[1]))
                    }
                    else
                        download.html(archivos[id].replace(play.data("play")[1],play.data("play")[0]))
                else
                    download.html(archivos[id])

                download.toggleClass("bottom",(download.prop("scrollHeight")>download.outerHeight()));
            }
            else //sino se carga
            {
                cargando150=rotar($(cargando150).data("id",id),false); //se le añade el id para no cargarlo varias veces
                download.removeClass("active").html(rotar(cargando150,true));
                parar_peticiones(); //abortar otra peticion ajax de archivo si se estuviera enviando
                peticiones["descarga"]=$.ajax("../download_ajax/"+id+"/"+$("h3 a",file).attr("title")).done(function(data)
                {
                    //sino se hace antes se machaca al hacer el html en download
                    cargando150=rotar(cargando150,false);
                    if($(".thumblink",file).data("active_play")) //si se ha pulsado en el link de previsualizacion se activa en el embed
                    {
                        $(".thumblink",file).data("active_play",false);
                        data["html"]=data["html"].replace(data["play"][0],data["play"][1])
                    }
                    $(".thumblink",file).data("play",data["play"]);
                    if($('#results li.active h3 a').data("id")==id) //if paranoico para comprobar que se carga donde debe
                        download.html(data["html"]).addClass("active").toggleClass("bottom",(download.prop("scrollHeight")>download.outerHeight()));

                    archivos[id]=data["html"];
                }).fail(function(data)
                {
                    if(data.readyState!=0) //necesario por si se aborta, que se ejecuta esto despues de meter los ojos
                    {
                        cargando150=rotar(cargando150,false);
                        var error=$("#errorhandler",data.responseText);
                        $("a:last",error).remove();
                        archivos[id]=download.html('<article data-id="'+id+'">'+error.html()+'</article>').html();
                    }
                    //TODO no sale el enlace de cerrar ventana
                });
            }
        }
        else //sino solo se indica que esta activo
            download.addClass("active");

        if(ancho>=ancho_max_download) //fondo gris si no estaba active a la derecha
            $("#fondo").data("visible",false);
        else
            $("#fondo").fadeTo(200,.5).data("visible",true);

        //flecha de union
        $("#subcontent").animate({width:ancho-20},{duration:200,queue:false});
        $("#flecha").removeClass("return").css({top:file.position().top+5}).show(100)
        .animate({right:ancho>ancho_max_download+80?posicion_flecha+60:ancho>ancho_max_download?posicion_flecha+ancho-ancho_max_download-20:ancho<ancho_min_download?posicion_flecha-10:posicion_flecha-20},{duration:200,queue:false});
        //quitar otro resultado que estuviera activo y activar el actual
        $("#results li").removeClass("active").css("z-index",0);
        file.addClass("active").css("z-index",5);
        //como esta en position fixed se limita la posicion ya que no se puede hacer por css
        download.css({position:"fixed",top:arriba<121?121-arriba:0,height:alto-70}).removeClass("top").show(100)
        .animate({right:ancho>ancho_max_download?ancho-ancho_max_download:ancho<ancho_min_download?ancho-ancho_min_download:0},{duration:200,queue:false});
    }
    else if(ocul)
        ocultar();
}
//oculta la ventana de download
function ocultar(duracion,derecha)
{
    if(typeof(duracion)!="number")
        duracion=200;

    $("#fondo").fadeOut(duracion).data("visible",false);
    if(!$("#results:empty").length && !derecha) //dejar a la derecha de los resultados si no es una busqueda
    {
        $("#subcontent").animate({"width":ancho_max_download},{duration:duracion,queue:false});
        $("#flecha").animate({right:posicion_flecha},{duration:duracion,queue:false}).addClass("return"); //necesario por poner el subcontent -20 de ancho
        $("#download").css({position:"fixed",top:arriba<130?$("#subcontent").position().top-arriba:0}).animate({right:ancho-ancho_max_download},{duration:duracion,queue:false});
    }
    else //ocultar a la derecha completamente
    {
        cargando150=rotar(cargando150,false);
        parar_peticiones();
        $("#results li").removeClass("active");
        $("#flecha").hide(duracion).animate({right:-125},{duration:duracion,queue:false});//,complete:function(){$(this).show()}});
        $("#download").hide(duracion).animate({right:-780},{duration:duracion,queue:false,complete:function(){$(this).removeClass("active").empty()}});
        delete filtros["d"];
        if(window.location.hash)
            window.location.hash="#!/"+search_string(false);
        else
            window.location.search=search_string(false);
    }
}
//rota los iconos de carga
function rotar(selector,inicio)
{
    if(inicio) //si hay que mostrarlo se rota
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
        for(var i=0;i<$(">img",selector).length;i++)
        {
            var r=$(">img:eq("+i+")",selector);
            if(r.data("rotacion")===true) //si no estan moviendose se rotan
                r.data("rotacion",rotacion(r));
        }
        return selector.show();
    }
    else //sino se para y se quita
    {
        for(var i=0;i<$(">img",selector).length;i++)
        {
            clearInterval($(">img:eq("+i+")",selector).data("rotacion"));
            $(">img:eq("+i+")",selector).data("rotacion",true); //estado inicializado
        }
        return selector.hide().detach();
    }
}
function size_slider(values,slider)
{
    var size,filesize={},i,max=$(slider).slider("option","max"),min=$(slider).slider("option","min");
    for(var j in values)
    {
        size=Math.pow(2,values[j]);
        i=parseInt(Math.floor(Math.log(size)/Math.log(1024)));
        filesize[j]=Math.round(size/Math.pow(1024,i),2)+' '+['Bytes','KiB','MiB','GiB'][i];
    }
    $("#size span").eq(0).text((values[0]==min?"< ":"")+filesize[0]);
    $("#size span").eq(1).text((values[1]==max?"> ":"")+filesize[1]);
    if(values==min+","+max) //si no hay nada especificado se borra (hace la conversion automatica de array a string)
        delete filtros["size"];
    else
        filtros["size"]=[values[0]==min?0:values[0], values[1]==max?50:values[1]];
}
$(function()
{
    alto=$(window).height();
    ancho=$(window).width();
    arriba=$(window).scrollTop();
    container=$("#subcontent").position().top;
    //change necesario para inicializar
    $("#size div,#quality").slider({min:17,max:34,values:[17,34],range:true,stop:search,change:function(e,ui){size_slider(ui.values,this)},slide:function(e,ui){size_slider(ui.values,this)}});
    search_params();
    //envio del formulario
    $('.searchbox').submit(function(e)
    {
        if(window.location.hash)
        {
            filtros["q"]=$("#q").val();
            search();
            e.preventDefault();
        }
        else
            $(":submit").attr("disabled","disabled")
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
                        pipe=id=="type"?"|":"";
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
                        filtros[id]+=(id=="type" && filtros[id]?"|":"")+$(this).data("filter");
                        //si es busqueda relacionada se pone en el formulario
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
        if(window.location.hash)
        {
            search();
            e.preventDefault();
        }
    });
    //ventana de descarga
    //si se pincha en cualquier parte de los resultados se oculta la ventana de descarga
    $('#results').click(ocultar).on("click",">li[id!=no_results]",function(e)
    {
        $(this).data("scroll-start", $(window).scrollTop());
        mostrar($("h3 a",this),true);
        e.preventDefault();
        e.stopPropagation();
    }).on("click","a",function(e)
    {
        e.stopPropagation();
        //si no se usa el Ctrl a la vez que se hace clic o no es el enlace del archivo
        if(!e.ctrlKey && $(this).parent("h3").length)
        {
            e.preventDefault();
            mostrar($(this),true);
        }
    }).on(
    {
        click:function(){$(this).data("active_play",true)},
        mouseenter:function()
        {
            animation=function(thumb)
            {
                if(thumb.length)
                    return setInterval(function()
                    {
                        var img=(parseInt(thumb.attr("src").substr(-1))+1)%thumb.data("servers").split("_").length;
                        thumb.attr("src","http://images"+thumb.data("servers").substr(img*3,2)+".foofind.com/"+thumb.data("id")+img);
                    },500);
            }
            thumb_animation=animation($("img",this));
        },
        mouseleave:function(){clearInterval(thumb_animation)},
    },'.thumblink');
    //si se desliza la ventana de download es necesaria esta verificacion if(e.pageX<=822 || e.pageX>=1505)
    $("#fondo").hoverIntent({sensitivity:1,over:ocultar,interval:50,out:ocultar,timeout:50});
    $("#flecha").click(function()
    {
        if($("#fondo").data("visible"))
            ocultar();
        else if($("+div>article",this).data("id"))
            mostrar($('li.active h3 a'),false)
    });
    $("#download").on("click","input",function(e){$(this).select();e.stopPropagation()}).on("click",">a",function(e)
    {
        e.stopPropagation();
        $(this).parents().removeClass("active");
        ocultar(200,true)
    })
    .click(function(){$(window).scrollTop($('#results li.active').data("scroll-start"));mostrar($('li.active h3 a'),false)})
    .hoverIntent({sensitivity:1,over:function(){mostrar($('li.active h3 a'),false)},out:function(){},interval:50});
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
    $(window).scroll(function(e)
    {
        arriba=$(window).scrollTop();
        //la paginacion continua se activa cuando se baja por los resultados pero antes de llegar al final de la pagina
        load_more_results();

        var active = $("#results li.active");
        if(active.length) //si hay item activo acomoda la pestaña de download en la parte de arriba
        {
            var download=$("#download");
            download.css({position:"fixed",top:arriba<container?container-arriba:0});
            if($("#fondo").data("visible") || ancho>ancho_max_download) //si esta desplegado ademas se regula la derecha
                download.css({right:ancho>ancho_max_download?ancho-ancho_max_download:ancho<ancho_min_download?ancho-ancho_min_download:0});

            // scroll dentro de la descarga
            var max_scroll=download.prop("scrollHeight")-download.outerHeight(); // calcula la cantidad de scroll que se puede hacer en la descarga
            if (max_scroll<=0)
                download.scrollTop(0); // reinicia el scrollTop por si se ha redimensionado la ventana
            else // calcula el scroll que se debe hacer
            {
                var pos=arriba-active.data("scroll-start"); //posicion del scroll interno respecto de donde se empezó
                var offset_top=container+(pos-arriba)<0?0:container+(pos-arriba); //saca scroll-start de pos
                download.scrollTop(arriba<container?0:pos<max_scroll+offset_top?pos-offset_top:max_scroll)
                .toggleClass("top",pos>2 && arriba>container).toggleClass("bottom",pos<=max_scroll-2+offset_top);
            }
        }
    }).resize(function()
    {
        alto=$(window).height();
        ancho=$(window).width();
        if($("#fondo").data("visible"))
        {
            $("#subcontent").css({width:ancho});
            $("#flecha").css({right:ancho>ancho_max_download && ancho-ancho_max_download>70?posicion_flecha+60:ancho<ancho_min_download?posicion_flecha-10:posicion_flecha});
            $("#download").css({position:"absolute",top:arriba<container?0:arriba-container,right:ancho>ancho_max_download && ancho-ancho_max_download>70?70:ancho<ancho_min_download?0:10,height:alto-70});
            if(ancho>=ancho_max_download) //si la ventana esta desplegada a la derecha
                $("#fondo").fadeOut(200).data("visible",false);
        }
        else
        {//TODO ancho_max_download+20
            $("#subcontent").css({width:ancho-10});
            $("#flecha").css({right:ancho>ancho_max_download && ancho-ancho_max_download>70?posicion_flecha+60:ancho<ancho_min_download?posicion_flecha-490:ancho-828});
            $("#download").css(
            {
                position:"absolute",
                top:arriba<container?0:arriba-container,
                right:ancho>ancho_max_download?ancho-ancho_max_download>70?70:ancho-ancho_max_download:ancho<ancho_min_download?-480:ancho-ancho_max_download,
                height:alto-70
            });
        }
    }).bind('hashchange',function() //actualiza la busqueda
    {
        _gaq.push(['_trackPageview',location.pathname+location.search+location.hash]);
        search_params();
        search();
    }).unload(function(){$(":input[disabled=disabled]").removeAttr("disabled")});
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
    if(window.location.hash)
        search();
    else
    {
        cargando=rotar($(".loading"),false);
        cargando150=rotar($("#download>div"),false);
        devueltos=$("#results>li").length;
        respuesta_total_found=$("#results").data("total_found");
        if("d" in filtros) //si en la URL venia un archivo se intenta mostrar directamente
            mostrar($('#results h3 a[data-id="'+filtros["d"]+'"]'));
    }
});
