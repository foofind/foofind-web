$(document).ready(function(){
    // Helpers
    var param=function(name){
        // Extrae un parametro GET de la URL
        var r=(new RegExp(name + '=' + '(.+?)(&|$)')).exec(location.search);
        if(r&&(r.length>1)) return decodeURI(r[1]);
        return null;
        },
        hexDecode=function(hex){
        // Decodifica bytes en hexadecimal
        var r=[], i=0;
        while(i<hex.length) r.push("0x" + hex.substring(i, i += 2));
        return String.fromCharCode.apply(window, r);
        },
        isFloat=function(cad){
            if(isNaN(parseFloat(cad))||(cad.indexOf("-")>0)||(cad.indexOf("+")>0))
                return false;
            for(var i=0,j=cad.length;i<j;i++)
                if("0123456789.".indexOf(cad.charAt(i)) < 0)
                    return false;
            return true;
        };
    // Mejora de checkboxes (cool_checkbox)
    var checkboxes=$("input[type=checkbox]"), cctested=false, ccwork=false;
    checkboxes.each(function(){
        var id=this.id, on="on",off="off",me=$(this);
        if(this.dataset){ // HTML5 data (internacionalizacion)
            if (this.dataset["cool_on"]) on = this.dataset["cool_on"];
            if (this.dataset["cool_off"]) off = this.dataset["cool_off"];
            }
        me.wrap("<span class=\"cool_checkbox\"></span>").parent()
            .append("<label for=\"" + id + "\"><span>" + on + "</span><span><span>/</span></span><span>" + off + "</span></label>");
        // Prueba de funcionalidad: click en label marca el checkbox
        if(!cctested){
            var oldv = me.attr("checked");
            $("label", me.parent()).first().click();
            ccwork = (me.attr("checked") != oldv);
            if(oldv) me.attr("checked", "checked");
            else me.removeAttr("checked");
            cctested = true;
            }
        // Workaround
        if(!ccwork)
            me.parent().click(function(){
                if(!me.disabled) me.attr("checked",!me.attr("checked"));
                });
        });
    // Control genérico de seleccionar todo
    $(".select_all_placeholder")
        .each(function(){
            var str_select_all="Select all", str_select_none="Select none", str_select_or=",",
                me=$(this), chks=me.closest("form").find("input[type=checkbox]"),
                showOrHide=function(){
                    var sa=(chks.filter(":checked:not(:disabled)").length<chks.filter(":not(:disabled)").length)?"show":"hide",
                        sn=(chks.filter(":checked:not(:disabled)").length>0)?"show":"hide",
                        ss=(sa=="hide"||sn=="hide")?"hide":"show";
                    me.children("a:first-child")[sa]();
                    me.children("a:last-child")[sn]().css("text-transform",(ss=="show")?"lowercase":"none");
                    me.children("span")[ss]();
                    };
            if(this.dataset){
                if(this.dataset["select_all"]) str_select_all = this.dataset["select_all"];
                if(this.dataset["select_none"]) str_select_none = this.dataset["select_none"];
                if(this.dataset["select_or"]) str_select_or = this.dataset["select_or"];
                }
            me.append('<a>'+str_select_all+'</a><span> '+str_select_or+' </span><a>'+str_select_none+'</a>');
            me.children("a:first-child").click(function(e){
                chks.filter(":not(:disabled)").attr("checked","checked");
                showOrHide();
                });
            me.children("a:last-child").click(function(e){
                chks.filter(":not(:disabled)").removeAttr("checked");
                showOrHide();
                });
            chks.change(showOrHide);
            showOrHide();
            });
    // Spinbutton
    $("input.integer")
        .each(function(){
            var me=$(this),
                step=(parseFloat(me.data("step"))||1),
                timeout=null;
                minval=(parseFloat(me.data("minval"))||-Infinity),
                maxval=(parseFloat(me.data("maxval"))||Infinity),
                increment=function(){
                    var value=parseFloat(me.attr("value"));
                    value = (value+step<=maxval)?(value+step):maxval;
                    me.attr("value", value)
                    me.data("last_value", value);
                    },
                decrement=function(){
                    var value=parseFloat(me.attr("value"));
                    value = (value-step>=minval)?(value-step):minval;
                    me.attr("value", value)
                    me.data("last_value", value);
                    },
                inverse=function(){
                    var value=parseFloat(me.attr("value"));
                    value = (minval<=-value)?-value:minval;
                    // Comprobación de rango
                    if((value>=minval)&&(value<=maxval)){
                        me.attr("value", value)
                        me.data("last_value", value);
                        }
                    },
                verify=function(){
                    var value=parseFloat(me.attr("value")), newval=null;
                    if(!isFloat(value)) newval=parseFloat(me.data("last_value"));
                    else if(value<minval) newval=minval;
                    else if(maxval<value) newval=maxval;
                    if(newval!==null){
                        me.attr("value", newval);
                        me.data("last_value", newval);
                        return false;
                        }
                    return true;
                    },
                wrap=$("<span></span>").append(
                    $("<input type=\"button\" value=\"+\"/>").click(increment),
                    $("<input type=\"button\" value=\"-\"/>").click(decrement)
                    );
            me.parents("form").submit(function(){
                // Validación
                if(!verify()) e.preventDefault();
                });
            me.data("last_value", me.attr("value")||0);
            me.focusout(function(){
                // Validación
                if(timeout) clearTimeout(timeout);
                timeout=null;
                verify();
                });
            me.keydown(function(e){
                // Control de entrada
                var code=e.keyCode,
                    character=String.fromCharCode(e.keyCode),
                    current=me.attr("value");
                if(e.ctrlKey||e.metaKey) return;
                else if(code<47){
                    // Caracteres no imprimibles
                    if(code==38){
                        // Arriba
                        increment();
                        e.preventDefault()
                        }
                    else if(code==40){
                        // Abajo
                        decrement();
                        e.preventDefault()
                        }
                    }
                else if(character=="-"){
                    // Convertir a negativo
                    if((minval<0)&&(parseFloat(current)>0)) inverse();
                    e.preventDefault()
                    }
                else if(character=="+"){
                    // Convertir a positivo
                    if((maxval>0)&&(parseFloat(current)<0)) inverse();
                    e.preventDefault();
                    }
                else if(!isFloat(current+character)){
                    // El valor resultante no es numérico
                    e.preventDefault();
                    }
                else{
                    if(current=="0") me.attr("value","");
                    if(timeout) clearTimeout(timeout);
                    timeout = setTimeout(function(){
                        timeout=null;
                        verify();
                        },1000);
                    }
                });
            me.wrap("<span class=\"spinbutton\"></span>").parent().append(wrap);
            });

    var c=function(x){
        var p=(x[0]=="/")?x.slice(1):x, s=p.search("/"), q=p.search("\\?");
        if(s > -1) p = p.slice(s + 1);
        if(q > -1) p = p.slice(0, q);
        return p;
        }, path=c(window.location.pathname), field_prefix="field_";

    // Log
    if(path=="admin"){
        var refresh_interval=null;
        if(($("#processing").attr("value")||"").toLowerCase()=="true"){
            var stdsep = hexDecode($("#log_data").data("stdsep")),
                n = parseInt($("#number").attr("value"))*($("#mode").attr("value")=="tail"?-1:1);
                log_refresh = function(){
                $.ajax({
                    url:"/en/admin/task/output",
                    data:{"log":param("show"),"n":n},
                    success:function(data){
                        var output = [];
                        if(data.cached) clearInterval(refresh_interval);
                        if(data.error) output.push(data.error);
                        for(var i in data)
                            if(data[i].join)
                                output.push(data[i].join("\n"));
                        $("#log_data").text(output.join(stdsep));
                        }
                    });
                };
            log_refresh();
            refresh_interval = setInterval(log_refresh, 500);
            }
        }
    // Bloqueo / información de ficheros
    else if(path.indexOf("admin/lockfiles")==0){
        var prefix="raw_";
        $("tr.raw_file_data td").each(function(){
             if(this.id&&(this.id.indexOf(prefix)==0)){
                var dialog = $("span", this)
                    .attr("title", "_id: "+ this.id.substr(prefix.length))
                    .dialog({
                        width:600,
                        height:400,
                        position:"center center",
                        modal:true,
                        autoOpen:false,
                        });
                $("<input type=\"button\" value=\"View raw data\"/>")
                    .click(function(){dialog.dialog("open");})
                    .appendTo(this);
                }
            });
        }
    // Deploy
    else if(path=="admin/deploy"){
        if($("input[type=submit]").attr("disabled")=="disabled"){ // AJAX polling de estado de deploy
            var inter=setInterval(function(){
                $.getJSON("/en/admin/deploy/status", function(data, textStatus){
                    if(textStatus=="success"){
                        if(data["available"]){
                            clearInterval(inter);
                            $(".in_progress_message").remove();
                            $("input[type=submit]").removeAttr("disabled");
                            }
                        $("#stderr").text(data["stderr"]);
                        $("#stdout").text(data["stdout"]);

                        // Actualiza lista de backups
                        var dl = $("#backup_list"), dd, section;
                        if(dl){
                            dl.empty();
                            for(var i0=0,j0=data["backups"].length;i0<j0;i0++){
                                section=data["backups"][i0];
                                dd = "";
                                dl.append('<dt>'+section[0]+'</dt>');
                                for(var i1=0,j1=section[1].length;i1<j1;i1++){
                                    dd = dd + "<a href=\"/en/admin/deploy/file/"+section[1][i1]+"\">"+section[1][i1]+"</a>";
                                    if(i1<j1-1) dd = dd + " | ";
                                    }
                                dl.append('<dd>'+dd+'</dd>');
                                }
                            }
                        // Actualiza la lista de publicables
                        var select=$("#publish_mode");
                        if(select){
                            select.empty();
                            for(var i0=0,j0=data["publish"].length;i0<j0;i0++){
                                section=data["publish"][i0];
                                select.append('<option value="'+section[0]+'">'+section[1]+'</option>');
                                }
                            select.children().first().attr("selected","selected");
                            }
                        }
                    });
                }, 2000);
            }
        else{
            // Ya ha terminado al cargar, se borra el mensaje con timeout
            setTimeout($(".in_progress_message").remove, 2000);
            }
        // Checkboxes de script
        /* Extrae los _view del select y los procesa por ajax, para mostrar
         * la salida en un <pre> por host.
         */
        var chk=$('select[name=script_mode]');
        if(chk){
            var available=null, h=$('#script_hosts'),
                status_unknown=(h.data("status_unknown")||"Unknown status"),
                status_loading=(h.data("status_loading")||"Loading"),
                status_processing=(h.data("status_processing")||"Processing"),
                view = {}, refresh_interval = null;
            // Extrae los _view del select y los registra
            $('select[name=script_mode] option').each(function(){
                var fi=this.value.lastIndexOf("/")+1, li=this.value.lastIndexOf("."), txt;
                if(li==-1) txt=this.value.slice(fi);
                else txt=this.value.slice(fi, li);
                if(txt.lastIndexOf("_view")==txt.length-5){
                    view[
                        this.value.slice(0, this.value.lastIndexOf("_")) +
                        this.value.slice(this.value.lastIndexOf("."))
                        ] = this.value;
                    $(this).remove();
                    }
                });
            // Añado los <pre>
            $("<pre>"+status+"</pre>").appendTo('#script_hosts li');
            // Cuando cambia el desplegable, se bloquean los checkbox y se llaman a los _view
            chk.change(function(){
                var value=chk.attr("value"), w=view[value], v;
                if(refresh_interval){
                    clearInterval(refresh_interval);
                    refresh_interval = null;
                    }
                if(available==null)
                    available = $.parseJSON($('input[name=script_available_hosts]').attr("value")||"{}");
                v = (available[value]||[]);
                // Desactivado de checkboxes y ocultación de pre
                $('#script_hosts input[type=checkbox]').each(function(){
                    var me=$(this), o=me.attr("disabled"), pre=$("pre", me.closest("li"));
                    if (v.indexOf(me.attr("value"))==-1){
                        me.attr("disabled","disabled");
                        pre.text("");
                        }
                    else{
                        if (me.attr("disabled")) me.removeAttr("disabled");
                        if(w) pre.text(status_loading);
                        else pre.text("");
                        }
                    if(o!=me.attr("disabled")) me.change(); // Si he activado o desactivado, ejecuto el evento change
                    });
                // Obtención de resultado de _view
                if(w){
                    var script_refresh = function(){
                        $.ajax({
                            url:"/en/admin/task/output",
                            data:{"script":w},
                            success:function(data){
                                var status=status_processing;
                                if(data.cached){
                                    status=status_unknown;
                                    clearInterval(refresh_interval);
                                    refresh_interval=null;
                                    }
                                $('#script_hosts input[type=checkbox]').each(function(){
                                    var lines=data[this.value]?data[this.value].join("\n"):status;
                                    if(v.indexOf(this.value)>-1)
                                        $("pre", $(this).closest("li")).text(lines);
                                    });
                                }
                            });
                        };
                    script_refresh();
                    refresh_interval = setInterval(script_refresh, 2000);
                    }
                }).change();
            }
        // Desplegable de botones de deploy
        var showbtn=$('<a href="javascript:" class="showmore button">▼</a>')
            .addClass("toggler")
            .click(function(){
                var cur=$(".deploy_advanced").css("display")=="none";
                $(".deploy_advanced").css("display",cur?"inherit":"none");
                showbtn.text(cur?"▲":"▼");
                });
        $(".deploy_advanced").css("display","none");
        $(".deploy_buttons")
            .append("<dd></dd>")
            .children(":last-child")
            .append(showbtn);
        // Dialogo de confirmacion para el submit
        var deployform=$("input[type=submit]#deploy").parents("form"),
            deploy_confirm=(deployform.data("confirmation_message")||"Are you sure you want to perform \"__action__\" on __target__?"),
            deployform_buttons=deployform.find("input[type=submit]"),
            action=null;
        deployform_buttons.click(function(){
            if($(this).data("noconfirm")) action=null;
            else action=this.value.toLowerCase();
            });
        deployform.submit(function(event){
            if (action&&(!confirm(deploy_confirm
                    .replace("__action__", action)
                    .replace("__target__", $("#mode").attr("value"))
                    ))){
                event.stopPropagation();
                event.preventDefault();
                }
            });
        }
    // Alternatives form
    else if(path.indexOf("admin/db/edit/alternatives")==0){
        var ds=function(prefix){
            var tr = $("form dd").filter(function(){
                var input=$("input, textarea, select", this);
                return (input.length>0)?(input.attr("name").indexOf(field_prefix+prefix)==0):false;
                });
            return tr.add(tr.prev("dt"));
            },
            current_methods=[],
            amethods=$("form input[name="+field_prefix+"available_methods]").attr("value").split(","),
            methods=$("form input[name="+field_prefix+"methods]"),
            showOrHide=function(){
                var t=methods.attr("value").trim()?methods.attr("value").split(","):[],p;
                for(var i=0,j=t.length;i<j;i++) t[i] = t[i].trim();
                current_methods = t;
                ds("param_").css("display",t.indexOf("param")>-1?"block":"none");
                ds("probability").css("display",t.indexOf("random")>-1?"block":"none");
                ds("remember_").css("display",t.indexOf("remember")>-1?"block":"none");
                $(".formbutton li", methods.parent()).css("text-decoration", function(i){
                    return current_methods.indexOf(amethods[i])>-1?"line-through":"none";});
            };

        // Method dialog
        $("form input[name="+field_prefix+"available_methods]").parent().hide().prev().hide();
        var lis="<div class=\"formbutton edit\"><a href=\"javascript:\" title=\"edit\"></a><ul class=\"dialog\">",
            btn, dialog;

        for(var i=0,j=amethods.length;i<j;i++){
            amethods[i] = amethods[i].trim();
            lis += "<li>"+amethods[i]+"</li>";
            }

        btn = $(lis + "</ul></div>");
        dialog = $("ul", btn);
        methods.parent().append(btn);
        methods.attr("readonly","readonly");

        // Methods input events
        $("li", btn).click(function(ev){
            var amethod=$(this).index();
            if (amethod>-1){
                amethod=amethods[amethod];
                if(current_methods.indexOf(amethod)==-1)
                    methods.attr("value", current_methods.length>0?methods.attr("value")+", "+amethod:amethod);
                else{
                    current_methods.splice(current_methods.indexOf(amethod),1);
                    methods.attr("value", current_methods.join(", "));
                    }
                showOrHide();
                }
            dialog.hide();ev.stopPropagation();ev.preventDefault();
            });
        $("a", btn).click(function(ev){dialog.show();ev.stopPropagation();ev.preventDefault();});
        $(document).click(function(){dialog.hide();});
        methods.change(showOrHide).keydown(function(){setTimeout(showOrHide,10);});
        showOrHide();
        }
    });
